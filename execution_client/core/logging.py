# Copyright (c) 2024, Huawei Technologies Co., Ltd.  All rights reserved.
import logging
import loguru
from loguru import logger
import os
import sys
import os.path as osp
from typing import Literal
import threading
try:
    import moxing as mox
    MOXING_ACTIVE = True
except ImportError:
    MOXING_ACTIVE = False


# Env var for passing facility name
RLXF_LOG_NAME = 'RLXF_LOG_NAME'
# Env var for passing relative log file path
RLXF_LOG_PATH = 'RLXF_LOG_PATH'
RLXF_SESSION_LOG_PATH = 'RLXF_SESSION_LOG_PATH'
# Env var storing local log root directory
RLXF_ROOT = 'RLXF_ROOT'
# Env Var storing S3 log root directory
RLXF_LOG_S3_DIR = 'RLXF_LOG_S3_DIR'
# log
TYPES = ["client", "service"]


def build_logger_env_vars(name, log_rel_path):
    """ Used to pass logger-related setup to child processes """

    root_dir = os.environ.get('RLXF_ROOT', os.getcwd())  # set in launcher, stays the same across all the processes

    return {
        RLXF_LOG_NAME: name,
        RLXF_ROOT: root_dir,
        RLXF_LOG_PATH: osp.join(root_dir, log_rel_path)
    }



class MoxingHandler(logging.Handler):
    """
    A handler class which writes logging records, appropriately formatted,
    to a S3 file
    """

    terminator = '\n'

    def __init__(self, s3_path):
        super().__init__()
        self.s3_path = s3_path
        mox.file.make_dirs(osp.dirname(s3_path))

    def flush(self):
        pass

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline.  If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.  If the stream
        has an 'encoding' attribute, it is used to determine how to do the
        output to the stream.
        """
        try:
            with mox.file.File(self.s3_path, "a") as f:
                msg = self.format(record)
                f.write(msg + self.terminator)
        except Exception:
            self.handleError(record)

    def __repr__(self):
        level = logging.getLevelName(self.level)
        name = getattr(self.stream, 'name', '')
        name = str(name)
        if name:
            name += ' '
        return f'<{self.__class__.__name__} {name}({level})>'


# 1. 创建一个自定义过滤器类
class SessionFilter(logging.Filter):
    def __init__(self):
        super(SessionFilter, self).__init__()

    def filter(self, record):

        if not hasattr(record, "session_id"):
            # 如果没有，使用生成器创建 session_id
            setattr(record, "session_id", None)
        if not hasattr(record, "span_id"):
            setattr(record, "span_id", None)
        return True


class SessionLogHandler(logging.Handler):

    def __init__(self, log_dir: str = None):
        logging.Handler.__init__(self)
        self.log_dir = log_dir or "./session_logs"
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir, exist_ok=True)

    def emit(self, record):
        """
        重写emit函数，将有session_id的日志记录到文件
        """
        try:
            msg = self.format(record)
            if record.session_id:
                file_dir = f'{self.log_dir}/{record.session_id}.log'
                with open(file_dir, 'a') as write:
                    write.write(msg + "\n")
        except Exception:
            self.handleError(record)


class SafeFormatter(logging.Formatter):

    def format(self, record):
        record.__dict__.setdefault('session_id', None)
        record.__dict__.setdefault('span_id',   None)
        return super().format(record)


class ManageLogger:
    def __init__(
            self,
            name: str = None,
            log_path: str = None,
            file_handler_type: str = None,
            is_use_loguru: bool = False,
            session_log_path: str = None
    ):
        self.name = name
        self.log_path = os.environ.get(RLXF_LOG_PATH, log_path)
        self.session_log_path = os.environ.get(RLXF_SESSION_LOG_PATH, session_log_path)
        self.s3_root = os.environ.get(RLXF_LOG_S3_DIR)
        self.file_handler_type = file_handler_type
        self.is_use_loguru = is_use_loguru
        self.log_path_replace()
        if self.is_use_loguru is False:
            self.conf_logging()
        else:
            self.conf_loguru()

    def log_path_replace(self):
        if self.log_path:
            if '%(pid)' in self.log_path:
                self.log_path = self.log_path.replace('%(pid)', str(os.getpid()) + f"_{self.name}")

    def conf_logging(self):
        level_str = os.getenv("LOGGING_LEVEL", "INFO")
        self.level = logging._nameToLevel.get(level_str, logging.INFO)
        # 模块级日志
        if self.name is not None:
            self.logger = logging.getLogger(self.name)
        # 根日志
        else:
            self.logger = logging.getLogger()
            self.logger.handlers.clear()

        # 避免重复配置
        if self.logger.handlers:
            return
        # 禁止向上传播，多次记录
        self.logger.propagate = False
        self.logger.setLevel(self.level)

        node_id = int(os.getenv("VC_TASK_INDEX", "0"))

        self.formatter = SafeFormatter(
            "[node_id:" + str(node_id) + "]"
            "%(asctime)s|%(filename)s:%(lineno)d|%(funcName)s|%(process)d|"
            "%(thread)d|%(threadName)s|%(levelname)s|%(session_id)s|%(span_id)s|%(message)s"
        )

        # 添加输出到 stderr 的处理器
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(self.level)
        stderr_handler.setFormatter(self.formatter)

        session_handler = SessionLogHandler(self.session_log_path)
        session_handler.setFormatter(self.formatter)

        self.logger.addHandler(stderr_handler)
        self.logger.addHandler(session_handler)

        session_filter = SessionFilter()
        self.logger.addFilter(session_filter)

        # 如果指定了文件名，添加输出到文件的处理器
        if self.log_path:
            # self.logger.info("Setup file logging to %s", self.log_path)
            os.makedirs(osp.dirname(self.log_path), exist_ok=True)
            if self.file_handler_type == "rotate":
                file_handler = logging.handlers.RotatingFileHandler(
                    self.log_path, maxBytes=100 * 1024 * 1024, backupCount=1000
                )
            else:
                file_handler = logging.FileHandler(self.log_path)
            file_handler.setLevel(self.level)
            file_handler.setFormatter(self.formatter)
            self.logger.addHandler(file_handler)

        if MOXING_ACTIVE and self.s3_root and self.log_path:
            s3_path = osp.join(self.s3_root, osp.basename(self.log_path))
            self.logger.info("Setup moxing logging to %s", s3_path)
            mox_handler = MoxingHandler(s3_path)
            mox_handler.setLevel(self.level)
            mox_handler.setFormatter(self.formatter)
            self.logger.addHandler(mox_handler)

        # Clean log without timestamps (controlled by RLXF_CLEAN_LOG_PATH env var)
        clean_log_path = os.environ.get("RLXF_CLEAN_LOG_PATH")
        if clean_log_path:
            os.makedirs(osp.dirname(clean_log_path) or ".", exist_ok=True)
            clean_formatter = SafeFormatter("%(levelname)s|%(message)s")
            clean_handler = logging.FileHandler(clean_log_path)
            clean_handler.setLevel(self.level)
            clean_handler.setFormatter(clean_formatter)
            self.logger.addHandler(clean_handler)

    def __call__(self, *args, **kwargs):
        return self.wrap_interface_msg(**kwargs)

    def get_logger(self):
        """
        返回配置好的日志记录器。
        """
        if self.is_use_loguru is False:
            if not hasattr(self.logger, "flush"):
                setattr(self.logger, "flush", self.flush)
            return self.logger
        else:
            setattr(logger, "flush", self.flush)
            return logger

    @staticmethod
    def wrap_interface_msg(
            typing: Literal["client", "service"],
            interface_name: str,
            params=None,
            response=None,
            result: str = None,
            exception: BaseException = None,
            request_id: str = "",
            duration: str = "",
    ) -> str:
        """
        统一客户端调用或服务端运行日志格式。
        """
        # 如果 result 为 None，表示开始调用或运行接口
        if typing == "client":
            if result is None:
                msg = f"Start to call <{interface_name}>, params={params}."
            elif result == "success":
                msg = f"End to call <{interface_name}>, result={result}, response={response}."
            elif result == "fail":
                msg = f"End to call <{interface_name}>, result={result}, exception={str(exception)}."
            else:
                msg = f"The call client record incoming error parameters."
        elif typing == "service":
            if result is None:
                msg = f"Start to run <{interface_name}>, params={params}."
            elif result == "success":
                msg = f"End to run <{interface_name}>, result={result}, response={response}."
            elif result == "fail":
                msg = f"End to run <{interface_name}>, result={result}, exception={str(exception)}."
            else:
                msg = f"The call service record incoming error parameters."
        else:
            msg = "The type of record must be 'client' or 'service'."

        if len(request_id) > 0:
            msg = f"[{request_id}]: {msg}"
        if len(duration) > 0:
            msg = f"{msg}:, duration={duration}"
        return msg

    # ray 训练接口特殊参数场景
    @staticmethod
    def format_msg(msg, iteration=None, steps=None) -> str:
        if iteration is not None and steps is not None:
            fmt_msg = f"iteration: {iteration} / {steps} | "
        else:
            fmt_msg = ""
        if isinstance(msg, dict) and iteration is not None and steps is not None:
            for key in msg:
                if isinstance(msg[key], int) or isinstance(msg[key], float) or len(msg[key]) == 1:
                    fmt_msg += f"{key} : {format(msg[key], '.4f')} | "
            fmt_msg = fmt_msg[:-2]
        else:
            fmt_msg += str(msg)
        return fmt_msg

    def flush(self):
        if self.is_use_loguru is False:
            for handler in self.logger.handlers:
                handler.flush()
        else:
            # 刷新所有处理器
            for handler in logger._core.handlers:
                if hasattr(handler, 'sink'):
                    handler.sink.flush()

    def conf_loguru(self):
        # 配置loguru的全局设置
        self.level = os.getenv("LOGGING_LEVEL", "INFO")
        loguru.logger.configure(
            handlers=[
                {"sink": sys.stderr, "level": self.level, "format": self._get_log_format()}
            ]
        )
        
        # 如果指定了文件路径，添加文件处理器
        if self.log_path:
            self._setup_file_handler(self.file_handler_type)
        
        # 如果MOXING_ACTIVE为真，添加S3处理器
        if MOXING_ACTIVE and self.s3_root and self.log_path:
            self._setup_s3_handler()

    def _get_log_format(self):
        node_id = int(os.getenv("VC_TASK_INDEX", "0"))
        return (
            "[node_id:" + str(node_id) + "]"
            "<green>{time:YYYY-MM-DD HH:mm:ss,SSS}</green>"
            "|<cyan>{file}</cyan>:<cyan>{line}</cyan>"
            "|<cyan>{function}</cyan>"
            "|<yellow>{process}</yellow>"
            "|<yellow>{thread}</yellow>"
            "|<yellow>{thread.name}</yellow>"
            "|<level>{level}</level>"
            "|<level>{message}</level>"
        )
    
    def _setup_file_handler(self, file_handler_type):
        if self.log_path:
            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        
        # 添加文件处理器
        if file_handler_type == "rotate":
            logger.add(
                self.log_path,
                rotation="100 MB",
                retention=10,
                level=self.level,
                format=self._get_log_format(),
                backtrace=True,
                enqueue=True,
            )
        else:
            logger.add(
                self.log_path,
                level=self.level,
                format=self._get_log_format(),
                backtrace=True,
                enqueue=True,
            )
    
    def _setup_s3_handler(self):
        # 构建S3路径
        self.s3_path = os.path.join(self.s3_root, self.log_path)
        mox.file.make_dirs(os.path.dirname(self.s3_path))
        logger.add(
            sink=self._moxing_sink,
            level=self.level,
            format=self._get_log_format(),
            serialize=False,
            backtrace=True,
            enqueue=True,
        )
    
    def _moxing_sink(self, message):
        record = message.record
        record['thread.name'] = record.get('thread.name', threading.current_thread().name)
        msg = f"{record['time']}|{record['file']}:{record['line']}|{record['function']}|{record['process']}|{record['thread']}|{record['thread.name']}|{record['level']}|{record['message']}\n"
        
        try:
            with mox.file.File(self.s3_path, "a") as f:
                f.write(msg)
        except Exception as e:
            logger.opt(exception=True).error("Failed to write to S3: {}", e)


class CodeInterpreterLogger:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, save_path=None) -> None:
        name="code_interpreter"
        if save_path is None:
            save_path = os.getenv("code_interpreter_log_path")
        if save_path:
            if any([
                not hasattr(self, "logger"),
                hasattr(self, "logger") and self.save_path is None,
            ]):
                log_path = os.path.join(
                    save_path,  # config.actor_rollout_ref.actor_rollout.save
                    "code_interpreter_log",
                    os.getenv("MA_VJ_NAME", "MA_VJ_NAME"),
                    f"{os.environ.get('VC_TASK_INDEX', '0')}.log"
                )
                logger = ManageLogger(
                    name=name, log_path=log_path, 
                    file_handler_type="rotate", is_use_loguru=True
                )
                self.logger = logger.get_logger()
                self.save_path = save_path
        elif not hasattr(self, "logger"):
            logger = ManageLogger(name=name, file_handler_type="rotate", is_use_loguru=True)
            self.logger = logger.get_logger()
            self.save_path = None

    def get_logger(self):
        return self.logger
