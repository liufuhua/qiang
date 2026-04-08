import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
except ImportError:
    pass

import pymysql
from typing import List, Dict, Optional
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import os

# 加载环境变量（复用你之前的配置）
load_dotenv()

# 全局单例
_ssh_tunnel: Optional[SSHTunnelForwarder] = None
_db_config: Optional[dict] = None


def getDB():
    """获取数据库连接，使用单例模式管理 SSH 隧道"""
    global _ssh_tunnel, _db_config

    # 1. 构建基础数据库配置（只做一次）
    if _db_config is None:
        _db_config = {
            "user": os.getenv("SHOP_DB_USER"),
            "password": os.getenv("SHOP_DB_PASSWORD"),
            "database": os.getenv("SHOP_DB_NAME"),
            "charset": "utf8mb4",
        }
        missing_db = [k for k, v in _db_config.items() if not v]
        if missing_db:
            raise RuntimeError(f"数据库核心配置缺失: {', '.join(missing_db)}")

        # 初始化 SSH 隧道（如果配置了）
        sshHost = os.getenv("SHOP_SSH_HOST")
        if sshHost:
            ssh_config = {
                "host": os.getenv("SHOP_SSH_HOST"),
                "port": os.getenv("SHOP_SSH_PORT"),
                "username": os.getenv("SHOP_SSH_USERNAME"),
                "pkey": os.getenv("SHOP_SSH_PKEY"),
                "db_remote_host": os.getenv("SHOP_DB_HOST"),
                "db_remote_port": os.getenv("SHOP_DB_PORT"),
                "local_host": os.getenv("SHOP_DB_LOCAL_HOST", "127.0.0.1"),
                "local_port": os.getenv("SHOP_DB_LOCAL_PORT")
            }

            # 检查SSH配置完整性
            missing = [k for k, v in ssh_config.items() if not v and k not in ["local_host"]]
            if missing:
                raise RuntimeError(f"SSH隧道已启用但配置缺失: {', '.join(missing)}")
            ssh_port = int(ssh_config["port"])
            db_remote_port = int(ssh_config["db_remote_port"])
            local_port = int(ssh_config["local_port"])

            # 创建并启动SSH隧道（仅一次）
            if _ssh_tunnel is None:
                _ssh_tunnel = SSHTunnelForwarder(
                    (ssh_config["host"], ssh_port),
                    ssh_username=ssh_config["username"],
                    ssh_pkey=ssh_config["pkey"],
                    remote_bind_address=(ssh_config["db_remote_host"], db_remote_port),
                    local_bind_address=(ssh_config["local_host"], local_port),
                )
                _ssh_tunnel.start()
                print(f"✅ SSH隧道已启动，本地转发端口：{_ssh_tunnel.local_bind_port}")

            _db_config["host"] = _ssh_tunnel.local_bind_host
            _db_config["port"] = _ssh_tunnel.local_bind_port
        else:
            # 直连模式
            if not os.getenv("SHOP_DB_HOST"):
                raise RuntimeError("未配置SSH隧道且未配置数据库直连地址(SHOP_DB_HOST)")
            _db_config["host"] = os.getenv("SHOP_DB_HOST")
            _db_config["port"] = int(os.getenv("SHOP_DB_PORT", 3306))

    # 每次创建新的数据库连接（pymysql 连接不是线程安全的）
    conn = pymysql.connect(**_db_config)
    return conn
