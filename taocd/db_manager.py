# db_manager.py
import os
from taocd.pgsql import PgsqlStore
import asyncio
from dotenv import load_dotenv

load_dotenv()  # 加载环境变量

HOST_IP = os.environ.get('HOST_IP', '')
ONE_API_HOST = os.environ.get('ONE_API_HOST', '')
# 获取数据库配置
def get_db_config():
    return {
        'user': 'subnet04',
        'password': 'subnet04',
        'database': 'subnet04',
        'host': '162.244.82.55',
        'port': 5433,
        'min_size': 1,
        'max_size': 6
    }

# 创建 PgsqlStore 实例
def get_store():
    db_config = get_db_config()
    return PgsqlStore(db_config)

# 全局数据库实例
# store = get_store()

def get_event_loop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop