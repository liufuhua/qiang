from dotenv import load_dotenv
import os
from typing import Optional
from sshtunnel import SSHTunnelForwarder
from langchain_community.utilities import SQLDatabase

# 加载环境变量
load_dotenv()


def create_ssh_tunnel() -> Optional[SSHTunnelForwarder]:
    """
    创建SSH隧道连接（若配置了SSH相关环境变量）

    Returns:
        Optional[SSHTunnelForwarder]: 启动后的SSH隧道实例，未配置则返回None
    Raises:
        RuntimeError: SSH配置不完整时抛出异常
    """
    ssh_host = os.getenv("SHOP_SSH_HOST")
    if not ssh_host:
        return None

    # 读取SSH相关环境变量
    ssh_config = {
        "SHOP_SSH_PORT": os.getenv("SHOP_SSH_PORT"),
        "SHOP_SSH_USERNAME": os.getenv("SHOP_SSH_USERNAME"),
        "SHOP_SSH_PKEY": os.getenv("SHOP_SSH_PKEY"),
        "SHOP_DB_HOST": os.getenv("SHOP_DB_HOST"),
        "SHOP_DB_PORT": os.getenv("SHOP_DB_PORT"),
        "SHOP_DB_LOCAL_HOST": os.getenv("SHOP_DB_LOCAL_HOST"),
        "SHOP_DB_LOCAL_PORT": os.getenv("SHOP_DB_LOCAL_PORT")
    }

    # 检查缺失的SSH配置
    missing_configs = [k for k, v in ssh_config.items() if not v]
    if missing_configs:
        raise RuntimeError(
            f"SSH隧道已启用但缺失环境变量: {', '.join(missing_configs)}"
        )

    # 转换端口为整数
    ssh_port = int(ssh_config["SHOP_SSH_PORT"])
    db_remote_port = int(ssh_config["SHOP_DB_PORT"])
    db_local_port = int(ssh_config["SHOP_DB_LOCAL_PORT"])

    # 创建并启动SSH隧道
    ssh_tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_config["SHOP_SSH_USERNAME"],
        ssh_pkey=ssh_config["SHOP_SSH_PKEY"],
        remote_bind_address=(ssh_config["SHOP_DB_HOST"], db_remote_port),
        local_bind_address=(ssh_config["SHOP_DB_LOCAL_HOST"], db_local_port),
    )
    ssh_tunnel.start()
    return ssh_tunnel


def get_database_instance() -> SQLDatabase:
    """
    获取SQLDatabase实例（支持SSH隧道或直接连接）

    Returns:
        SQLDatabase: 初始化后的数据库实例
    Raises:
        RuntimeError: 数据库配置不完整时抛出异常
    """
    # 1. 尝试创建SSH隧道
    ssh_tunnel = None
    try:
        ssh_tunnel = create_ssh_tunnel()

        # 2. 构建数据库连接URI
        db_uri = os.getenv("SHOP_DB_URI")
        if not db_uri:
            if not ssh_tunnel:
                raise RuntimeError("SHOP_DB_URI未配置且SSH隧道未启用")

            # 通过SSH隧道构建DB URI
            db_config = {
                "SHOP_DB_USER": os.getenv("SHOP_DB_USER"),
                "SHOP_DB_PASSWORD": os.getenv("SHOP_DB_PASSWORD"),
                "SHOP_DB_NAME": os.getenv("SHOP_DB_NAME")
            }
            missing_db_configs = [k for k, v in db_config.items() if not v]
            if missing_db_configs:
                raise RuntimeError(
                    f"SSH隧道模式下缺失数据库配置: {', '.join(missing_db_configs)}"
                )

            # 使用隧道的本地端口构建连接
            local_port = ssh_tunnel.local_bind_port
            db_uri = (
                f"mysql+pymysql://{db_config['SHOP_DB_USER']}:{db_config['SHOP_DB_PASSWORD']}"
                f"@127.0.0.1:{local_port}/{db_config['SHOP_DB_NAME']}?charset=utf8mb4"
            )

        # 3. 初始化并返回数据库实例
        db_instance = SQLDatabase.from_uri(
            db_uri,
            sample_rows_in_table_info=0,  # 不加载表数据样本，提升性能
        )
        return db_instance

    except Exception as e:
        # 若初始化失败，关闭SSH隧道并重新抛出异常
        if ssh_tunnel:
            ssh_tunnel.stop()
        raise e


# 对外暴露的数据库实例（单例模式）
db_instance = get_database_instance()

# 测试用例（可选）
if __name__ == "__main__":
    # 验证数据库连接
    try:
        # 获取所有表名，验证连接是否成功
        table_names = db_instance.get_usable_table_names()
        print(f"数据库连接成功！可用表名: {table_names}")
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")