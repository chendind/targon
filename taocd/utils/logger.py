from loguru import logger
import sys, os
from dotenv import load_dotenv
load_dotenv()
TAOCD_LOGER_PATH = os.environ.get('TAOCD_LOGER_PATH', 'logs')
if TAOCD_LOGER_PATH == '':
    TAOCD_LOGER_PATH = 'logs'
class TaocdLogger():
    def __init__(self, log_path: str = TAOCD_LOGER_PATH, log_file_prefix: str = 'taocd'):
        # 创建日志目录（如果不存在）
        log_path = os.path.join(os.getcwd(), log_path)
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        # 日志输出的文件格式
        full_log_path = os.path.join(log_path, log_file_prefix + '_{time:YYYY-MM-DD}.log')
        # 创建一个新的 logger 实例
        self.logger = logger.bind()
        self.logger.remove()
        # 添加日志处理程序，输出到文件，并按日切割
        logger.add(
            full_log_path,
            level="INFO",
            format="<yellow>{time:YYYY-MM-DD HH:mm:ss.SSS}</yellow> | <level>{level}</level> | {message}",
            rotation="00:00",
            retention="10 days",
            # compression="zip",
        )
        logger.add(
            sys.stdout,  # 输出到控制台
            level="INFO",
            format="<yellow>{time:YYYY-MM-DD HH:mm:ss}</yellow> | <level>{level}</level> | {message}"
        )

    def get_logger(self):
        """
        返回 logger 实例，供外部调用
        """
        return self.logger

