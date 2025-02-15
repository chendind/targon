import time, threading, random, json
from taocd.redis_client import redis_client
from taocd.utils.logger import TaocdLogger
logger = TaocdLogger(log_file_prefix='text_task_queue').get_logger()

class TextTaskQueue:
    def __init__(self):
        self.model_queue = {}
        self.start_schedule()

    def add(self, model_name, worker_name, task_md5, max_tokens):
        if model_name not in self.model_queue:
            self.model_queue[model_name] = {}
        if worker_name not in self.model_queue[model_name]:
            self.model_queue[model_name][worker_name] = {}
        self.model_queue[model_name][worker_name][task_md5] = {
            "max_tokens": max_tokens,
            "created_at": time.time()
        }
        
    def add_to_least_busy_worker(self, model_name, task_md5, max_tokens, is_chat_request):
        worker_name = self.get_least_busy_worker_name(model_name, is_chat_request)
        if worker_name is None:
            return None
        self.add(model_name, worker_name, task_md5, max_tokens)
        host_port = worker_name.split('@')[1]
        return {
            "queue_name": model_name,
            "worker_name": worker_name,
            "address": self.get_address(host_port, is_chat_request)
        }


    def remove(self, model_name, worker_name, task_md5):
        if model_name in self.model_queue:
            if worker_name in self.model_queue[model_name]:
                if task_md5 in self.model_queue[model_name][worker_name]:
                    del self.model_queue[model_name][worker_name][task_md5]

    def get_least_busy_worker_name(self, model_name, is_chat_request):
        least_busy_worker_name = None
        least_busy_value = float('inf')
        current_time = time.time()
        if model_name not in self.model_queue:
            return None
        
        queue_data = self.model_queue[model_name]
        # 遍历 queue_data 中的每个任务
        worker_items = list(queue_data.items())
        random.shuffle(worker_items)
        for worker_name, worker_info in worker_items:
            busy_value = 0
            for task_md5, task_info in worker_info.items():
                max_tokens = task_info["max_tokens"]
                created_at = task_info["created_at"]
                time_diff = current_time - created_at
                decay_base = self.get_decay_base(model_name, is_chat_request)
                weight = max(1 - time_diff / decay_base, 0.1)
                busy_value += max_tokens * weight
            logger.info(f'worker {worker_name} 的繁忙值为 {busy_value}')
            if busy_value < least_busy_value:
                least_busy_value = busy_value
                least_busy_worker_name = worker_name
        logger.info(f'worker {least_busy_worker_name} 是当前最空闲的')
        # 当所有 worker 都没有任务时，随机选择一个 worker
        if least_busy_worker_name is None and queue_data:
            least_busy_worker_name = random.choice(list(queue_data.keys()))
            return least_busy_worker_name
        return least_busy_worker_name
    
    def get_decay_base(self, model_name, is_chat_request):
        decay_bases = {
            "unsloth/Meta-Llama-3.1-8B-Instruct": (1.7, 11.0),
            "NousResearch/Hermes-3-Llama-3.1-8B": (1.7, 11.0),
            "deepseek-ai/deepseek-coder-33b-instruct": (8.0, 12.0),
            "nvidia/Llama-3.1-Nemotron-70B-Instruct-HF": (5.0, 25.0),
            "EnvyIrys/EnvyIrys_sn111_14": (4.0, 20.0),
            "deepseek-ai/DeepSeek-R1-Distill-Llama-70B": (5.0, 25.0)
        }
        chat_decay, non_chat_decay = decay_bases.get(model_name, (10.0, 10.0))
        return chat_decay if is_chat_request else non_chat_decay

    def get_address(self, host_port, is_chat_request):
        address = ''
        if is_chat_request:
            address = f'http://{host_port}/v1/chat/completions'
        else:
            address = f'http://{host_port}/v1/completions'
        return address

    def cleanup_old_tasks(self):
        current_time = time.time()
        for model_name, workers in self.model_queue.items():
            for worker_name, tasks in workers.items():
                tasks_to_remove = [
                    task_md5 for task_md5, task_info in tasks.items()
                    if current_time - task_info["created_at"] > 600
                ]
                for task_md5 in tasks_to_remove:
                    logger.info(f"Removing task {task_md5} from {worker_name}")
                    del tasks[task_md5]
    
    def load_worker_from_redis(self):
        """从 Redis 中加载 worker_name，并删除已经不存在的 worker_name"""
        # 从 Redis 中获取最新的 Worker 列表
        vllm_server_list_raw = redis_client.safe_lrange('vllm:server:list', 0, -1)
        if not vllm_server_list_raw:
            return

        try:
            vllm_server_list = [json.loads(item) for item in vllm_server_list_raw]
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse vllm_server_list: {e}")
            return

        # 构建当前 Redis 中的 Worker 集合
        current_workers = set()
        for vllm_server in vllm_server_list:
            try:
                model_name = vllm_server['model_name']
                worker_name = f"{model_name}@{vllm_server['host']}:{vllm_server['port']}"
                current_workers.add((model_name, worker_name))  # 使用 (model_name, worker_name) 作为唯一标识

                # 如果 model_name 不存在，初始化
                if model_name not in self.model_queue:
                    self.model_queue[model_name] = {}

                # 如果 worker_name 不存在，添加到 model_queue
                if worker_name not in self.model_queue[model_name]:
                    self.model_queue[model_name][worker_name] = {}
                    logger.info(f"Loaded worker {worker_name}")
            except KeyError as e:
                logger.error(f"Missing key in vllm_server: {json.dumps(vllm_server)}")
                continue

        # 删除 Redis 中已经不存在的 Worker
        for model_name in list(self.model_queue.keys()):
            for worker_name in list(self.model_queue[model_name].keys()):
                if (model_name, worker_name) not in current_workers:
                    del self.model_queue[model_name][worker_name]
                    logger.info(f"Removed worker {worker_name} from model {model_name}")

        logger.info(f"Updated self.model_queue: {json.dumps(self.model_queue)}")

        

    def start_schedule(self):
        self.stop_event = threading.Event()

        def run_cleanup():
            while not self.stop_event.is_set():
                self.cleanup_old_tasks()
                self.stop_event.wait(60)  # 每 60 秒执行一次

        def run_load_worker():
            while not self.stop_event.is_set():
                self.load_worker_from_redis()
                self.stop_event.wait(5)  # 每 5 秒执行一次

        threading.Thread(target=run_cleanup, daemon=True).start()
        threading.Thread(target=run_load_worker, daemon=True).start()