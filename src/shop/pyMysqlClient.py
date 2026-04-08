import pymysql
from typing import List, Dict, Optional
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import os

# 加载环境变量（复用你之前的配置）
load_dotenv()

class MySQLSSHClient:
    def __init__(self):
        self.ssh_tunnel: Optional[SSHTunnelForwarder] = None
        self.conn: Optional[pymysql.connections.Connection] = None
        # 初始化时判断是否启用SSH
        self.use_ssh = bool(os.getenv("SHOP_SSH_HOST"))

    def _create_ssh_tunnel(self) -> SSHTunnelForwarder:
        """创建SSH隧道（仅当配置了SHOP_SSH_HOST时调用）"""
        # 读取SSH配置
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

        # 转换端口为整数
        ssh_port = int(ssh_config["port"])
        db_remote_port = int(ssh_config["db_remote_port"])
        local_port = int(ssh_config["local_port"])

        # 创建并启动SSH隧道
        tunnel = SSHTunnelForwarder(
            (ssh_config["host"], ssh_port),
            ssh_username=ssh_config["username"],
            ssh_pkey=ssh_config["pkey"],  # 支持私钥路径或私钥字符串
            remote_bind_address=(ssh_config["db_remote_host"], db_remote_port),
            local_bind_address=(ssh_config["local_host"], local_port),
        )
        tunnel.start()
        print(f"✅ SSH隧道已启动，本地转发端口：{tunnel.local_bind_port}")
        return tunnel

    def connect(self) -> pymysql.connections.Connection:
        """
        自动判断连接方式：
        - 配置SHOP_SSH_HOST → SSH隧道连接
        - 未配置 → 直接连接数据库
        """
        # 1. 构建基础数据库配置
        db_config = {
            "user": os.getenv("SHOP_DB_USER"),
            "password": os.getenv("SHOP_DB_PASSWORD"),
            "database": os.getenv("SHOP_DB_NAME"),
            "charset": "utf8mb4",
            # "cursorclass": pymysql.cursors.DictCursor  # 返回字典格式结果
        }

        # 2. 检查核心数据库配置
        missing_db = [k for k, v in db_config.items() if not v and k != "cursorclass"]
        if missing_db:
            raise RuntimeError(f"数据库核心配置缺失: {', '.join(missing_db)}")

        # 3. SSH隧道模式
        if self.use_ssh:
            self.ssh_tunnel = self._create_ssh_tunnel()
            db_config["host"] = self.ssh_tunnel.local_bind_host
            db_config["port"] = self.ssh_tunnel.local_bind_port
        # 4. 直连模式
        else:
            # 检查直连地址配置
            if not os.getenv("SHOP_DB_HOST"):
                raise RuntimeError("未配置SSH隧道且未配置数据库直连地址(SHOP_DB_HOST)")
            db_config["host"] = os.getenv("SHOP_DB_HOST")
            db_config["port"] = int(os.getenv("SHOP_DB_PORT", 3306))

        # 5. 建立数据库连接
        self.conn = pymysql.connect(**db_config)
        print(f"✅ 数据库连接成功（{'SSH隧道' if self.use_ssh else '直连'}模式）")
        return self.conn

    def run_sql(self, sql: str) -> List[Dict]:
        """执行SQL并返回结构化结果（字典列表）"""
        if not self.conn:
            self.connect()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"SQL执行失败: {str(e)}")

    def close(self):
        """关闭数据库连接和SSH隧道"""
        # 关闭数据库连接
        if self.conn and not self.conn._closed:
            self.conn.close()
            print("✅ 数据库连接已关闭")
        # 关闭SSH隧道（仅当启用时）
        if self.ssh_tunnel and self.ssh_tunnel.is_alive:
            self.ssh_tunnel.stop()
            print("✅ SSH隧道已关闭")

    def __del__(self):
        """析构函数：自动关闭连接"""
        self.close()

# ------------------------------
# 简化的客户端创建函数（一键生成）
# ------------------------------
def get_mysql_client() -> MySQLSSHClient:
    """
    快速获取数据库客户端（自动判断SSH/直连）
    调用示例：
    client = get_mysql_client()
    result = client.run_sql("DESC order")
    """
    client = MySQLSSHClient()
    client.connect()
    return client

# ------------------------------
# 测试使用示例
# ------------------------------
if __name__ == "__main__":
    # 1. 一键创建客户端（自动判断连接方式）
    client = get_mysql_client()

    try:
        # 2. 执行DESC获取表结构
        desc_result = client.run_sql("DESC activity")
        print("\n=== DESC order 结构化结果 ===")
        for field in desc_result:
            print(f"字段名：{field['Field']} | 类型：{field['Type']} | 索引：{field['Key']}")

        # 3. 执行查询获取字段备注
        comment_sql = """
        SELECT COLUMN_NAME, COLUMN_COMMENT 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'order'
        """
        comment_result = client.run_sql(comment_sql)
        print("\n=== 字段备注 ===")
        for item in comment_result:
            print(f"{item['COLUMN_NAME']}: {item['COLUMN_COMMENT']}")

    except Exception as e:
        print(f"❌ 执行失败: {str(e)}")
    finally:
        client.close()