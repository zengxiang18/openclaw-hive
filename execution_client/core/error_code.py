

class ErrorCode:
    SUCCESS = (200, '成功')
    UNAUTHORIZED = (401, '未认证')
    FORBIDDEN = (403, '无权限')
    TIMEOUT = (408, "请求超时")
    OVERFLOW = (429, '超出访问次数限制！')

    # 自定义code
    SYSTEM_ERROR = (1001, '服务器开小差了，请稍后重试~')
    PARAMS_ERROR = (1002, "参数错误")
    PARAMS_MISS = (1003, "参数缺失")
    UNKNOWN_ERROR = (1004, "未知错误")
