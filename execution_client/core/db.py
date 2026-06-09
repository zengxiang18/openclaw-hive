import sqlite3
import datetime
from typing import Optional, Tuple

# 数据库连接配置（文件不存在则自动创建）
DB_PATH = "env_info.db"

def init_db() -> None:
    """初始化数据库，创建env_info表"""
    # 连接数据库（内存模式用 :memory:，文件模式用具体路径）
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建表：
    # env_id: env_idID（主键，唯一标识）
    # use_status: 状态（整数，如0-正常、1-禁用、2-待审核）
    # create_time: 创建时间（默认当前时间）
    # del_time: 删除时间（NULL表示未删除，软删除用）
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS env_info (
        env_id TEXT PRIMARY KEY NOT NULL,
        use_status INTEGER NOT NULL DEFAULT 0,
        create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        del_time TIMESTAMP
    );
    """
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print("表创建成功（或已存在）")
    except sqlite3.Error as e:
        print(f"创建表失败：{e}")
    finally:
        # 关闭游标和连接
        cursor.close()
        conn.close()

def add_env_info(env_id: str) -> bool:
    """
    新增env_id记录
    :param env_id: env_idID（必须唯一）
    :param use_status: 初始状态，默认0（正常）
    :return: 新增成功返回True，失败返回False
    """
    use_status = 1
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # 获取当前时间（格式：YYYY-MM-DD HH:MM:SS）
    create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    insert_sql = """
    INSERT INTO env_info (env_id, use_status, create_time)
    VALUES (?, ?, ?);
    """
    try:
        cursor.execute(insert_sql, (env_id, use_status, create_time))
        conn.commit()
        print(f"env_id:{env_id}新增成功")
        return True
    except sqlite3.IntegrityError:
        print(f"env_id:{env_id}已存在，新增失败（主键重复）")
        return False
    except sqlite3.Error as e:
        print(f"新增env_id失败：{e}")
        return False
    finally:
        cursor.close()
        conn.close()

def get_env_info(env_id: int) -> Optional[Tuple[int, int, str, Optional[str]]]:
    """
    查询env_id信息
    :param env_id: env_idID
    :return: (env_id, use_status, create_time, del_time)，不存在返回None
    """
    conn = sqlite3.connect(DB_PATH)
    # 启用列名映射（方便通过字段名取值）
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    select_sql = "SELECT * FROM env_info WHERE env_id = ? AND del_time IS NULL;"
    try:
        cursor.execute(select_sql, (env_id,))
        row = cursor.fetchone()
        if row:
            # 转换为元组返回
            return (row["env_id"], row["use_status"], row["create_time"], row["del_time"])
        else:
            print(f"env_id{env_id}不存在或已删除")
            return None
    except sqlite3.Error as e:
        print(f"查询env_id失败：{e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_all_use_env_id(use_status: int=1) -> Optional[Tuple[int, int, str, Optional[str]]]:
    """
    查询env_id信息
    :param env_id: env_idID
    :return: (env_id, use_status, create_time, del_time)，不存在返回None
    """
    conn = sqlite3.connect(DB_PATH)
    # 启用列名映射（方便通过字段名取值）
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    select_sql = "SELECT * FROM env_info WHERE use_status = ? AND del_time IS NULL;"
    try:
        cursor.execute(select_sql, (use_status,))
        rows = cursor.fetchall()
        if rows:
            # 转换为元组返回
            return [row["env_id"] for row in rows]
        else:
            return []
    except sqlite3.Error as e:
        print(f"查询use_status失败：{e}")
        return []
    finally:
        cursor.close()
        conn.close()

def soft_delete_env_info(env_id: str) -> bool:
    """
    软删除env_id（更新del_time，不物理删除）
    :param env_id: env_idID
    :return: 删除成功返回True
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    del_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    update_sql = """
    UPDATE env_info SET use_status = ?,del_time = ? WHERE env_id = ? AND del_time IS NULL;
    """
    try:
        cursor.execute(update_sql, (0, del_time, env_id))
        conn.commit()
        if cursor.rowcount > 0:
            print(f"env_id{env_id}软删除成功")
            return True
        else:
            print(f"env_id{env_id}不存在或已删除")
            return False
    except sqlite3.Error as e:
        print(f"删除env_id失败：{e}")
        return False
    finally:
        cursor.close()
        conn.close()

# 测试代码
if __name__ == "__main__":
    # 1. 初始化数据库（创建表）
    init_db()
    
    # # 2. 新增env_id
    # add_env_info("c1a9cff375614a6ca0a31f26c2e35c8f")  # 新增env_id1001，状态正常
    # add_env_info("158213612ad545d691b7e741e8bca439")  # 新增env_id1002，状态禁用
    
    # # 3. 查询env_id
    # env_info = get_env_info("c1a9cff375614a6ca0a31f26c2e35c8f")
    # if env_info:
    #     print("env_id信息：", env_info)
    
    # # 4. 软删除env_id
    # soft_delete_env_info("158213612ad545d691b7e741e8bca439")
    
    # # 5. 再次查询已删除的env_id
    # get_env_info("158213612ad545d691b7e741e8bca439")
    get_all_use_env_id(1)
