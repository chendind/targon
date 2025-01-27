import asyncpg, json, base64, asyncio
from taocd.utils.logger import TaocdLogger
from datetime import datetime

logger = TaocdLogger(log_file_prefix='pgsql').get_logger()

def with_db_retry_and_logging():
    def decorator(func):
        async def wrapper(self, *args, **kwargs):
            try:
                if not self.pool:
                    await self.connect()
                return await func(self, *args, **kwargs)
            except Exception as e:
                logger.info(f"[ERROR]Unhandled error in {func.__name__}: {e}", exc_info=True)
                raise
        return wrapper
    return decorator

class PgsqlStore:
    def __init__(self, db_config):
        self.db_config = db_config
        self.pool = None
        self.in_connection = False

    async def connect(self, max_retries=5):
        # 防止多次连接
        # if self.in_connection:
        #     return
        retries = 0
        while retries < max_retries:
            self.in_connection = True
            try:
                if not self.pool:
                    self.pool = await asyncpg.create_pool(
                        **self.db_config,
                        max_inactive_connection_lifetime=60
                    )
                    logger.info("Database connection pool created successfully.")
                else:
                    # 测试连接是否有效
                    async with self.pool.acquire() as connection:
                        await connection.execute("SELECT 1;")
                        logger.info("Database connection pool is healthy.")
                        self.in_connection = False
                        return
            except (asyncpg.PostgresError, OSError) as e:
                logger.error(f"Database connection error: {e}. Reconnecting...")
                retries += 1
                await asyncio.sleep(2)
        self.in_connection = False
        logger.error("Max retries reached. Could not connect to the database.")
        raise ConnectionError("Failed to connect to the database after multiple attempts.")

    @with_db_retry_and_logging()
    async def insert_task_with_images(self, client_ip, body_dict, celery_task_id, task_md5, post_endpoint, queue, hotkeys, worker_name, received_time, exec_time, host_ip):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # 初始化图像 ID
                init_image_id = None
                mask_image_id = None

                # 处理 init_image
                init_image = body_dict.get('init_image')
                init_image_str = init_image[:10] if init_image is not None else None
                logger.info(f"init_image: {init_image_str}")
                if init_image:
                    init_image_id = await self.insert_task_image({
                        "image_b64": init_image
                    })
                    pass

                # 处理 mask_image
                mask_image = body_dict.get('mask_image')
                if mask_image:
                    # mask_image_id = await self.insert_task_image({
                    #     "image_b64": mask_image
                    # })
                    pass

                # 准备任务数据
                task_data = {
                    "client_ip": client_ip,
                    "vali_hk": hotkeys.get('vali_hk'),
                    "miner_hk": hotkeys.get('miner_hk'),
                    "post_endpoint": post_endpoint,
                    "queue": queue,
                    "prompt": body_dict.get('prompt'),
                    "negative_prompt": body_dict.get('negative_prompt'),
                    "messages": json.dumps(body_dict.get('messages')) if body_dict.get('messages') else None,
                    "temperature": body_dict.get('temperature'),
                    "seed": body_dict.get('seed'),
                    "model": body_dict.get('model'),
                    "stream": body_dict.get('stream'),
                    "logprobs": body_dict.get('logprobs'),
                    "top_p": body_dict.get('top_p'),
                    "top_k": body_dict.get('top_k'),
                    "max_tokens": body_dict.get('max_tokens'),
                    "steps": body_dict.get('steps'),
                    "cfg_scale": body_dict.get('cfg_scale'),
                    "width": body_dict.get('width'),
                    "height": body_dict.get('height'),
                    "image_strength": body_dict.get('image_strength'),
                    "ipadapter_strength": body_dict.get('ipadapter_strength'),
                    "control_strength": body_dict.get('control_strength'),
                    "init_image_id": init_image_id,
                    "mask_image_id": mask_image_id,
                    "celery_task_id": celery_task_id,
                    "task_md5": task_md5,
                    "worker_name": worker_name,
                    "received_time": datetime.fromtimestamp(received_time),
                    "exec_time": exec_time,
                    "host_ip": host_ip
                }
                logger.info(f"task_data: {task_data}")
                # 插入任务
                task_db_id = await self.insert_task(task_data)

                return task_db_id
    
    @with_db_retry_and_logging()
    async def insert_task(self, task_data):
        async with self.pool.acquire() as connection:
            insert_query = """
            INSERT INTO task (
                client_ip,
                vali_hk,
                miner_hk,
                post_endpoint,
                queue,
                prompt,
                negative_prompt,
                messages,
                temperature,
                seed,
                model,
                stream,
                logprobs,
                top_p,
                top_k,
                max_tokens,
                steps,
                cfg_scale,
                width,
                height,
                image_strength,
                ipadapter_strength,
                control_strength,
                init_image_id,
                mask_image_id,
                celery_task_id,
                task_md5,
                worker_name,
                received_time,
                exec_time,
                host_ip,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            RETURNING id;
            """
            
            values = (
                task_data['client_ip'],
                task_data['vali_hk'],
                task_data['miner_hk'],
                task_data['post_endpoint'],
                task_data['queue'],
                task_data['prompt'],
                task_data['negative_prompt'],
                task_data['messages'],
                task_data['temperature'],
                task_data['seed'],
                task_data['model'],
                task_data['stream'],
                task_data['logprobs'],
                task_data['top_p'],
                task_data['top_k'],
                task_data['max_tokens'],
                task_data['steps'],
                task_data['cfg_scale'],
                task_data['width'],
                task_data['height'],
                task_data['image_strength'],
                task_data['ipadapter_strength'],
                task_data['control_strength'],
                task_data['init_image_id'],
                task_data['mask_image_id'],
                task_data['celery_task_id'],
                task_data['task_md5'],
                task_data['worker_name'],
                task_data['received_time'],
                task_data['exec_time'],
                task_data['host_ip']
            )
            
            return await connection.fetchval(insert_query, *values)

    @with_db_retry_and_logging()
    async def insert_task_image(self, image_data):
        async with self.pool.acquire() as connection:
            insert_query = """
            INSERT INTO task_image (image_b64, image_b64_save_status, created_at, updated_at)
            VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id;
            """
            image_bytes = base64.b64decode(image_data['image_b64'])
            # 将参数传递为元组，确保顺序正确
            return await connection.fetchval(insert_query, image_bytes, 1)

    @with_db_retry_and_logging()
    async def insert_task_time_records(self, task_id, time_records):
        async with self.pool.acquire() as connection:
            async with connection.transaction():

                # 创建一个包含正确顺序的元组列表
                time_records_with_task_id = [
                    (task_id, time_record['step_name'], time_record['exec_time'], datetime.fromtimestamp(time_record['start_time']))
                    for time_record in time_records
                ]
                insert_query = """
                INSERT INTO task_time_record (task_id, step_name, exec_time, start_time, created_at, updated_at)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                """
                await connection.executemany(insert_query, time_records_with_task_id)

    @with_db_retry_and_logging()
    async def insert_task_response(self, task_id, result, result_from, result_memo, task_time_consumption):
        async with self.pool.acquire() as connection:
            image_b64 = result.get('image_b64')
            task_image_id = None
            if image_b64:
                # task_image_id = await self.insert_task_image({
                #     "image_b64": image_b64
                # })
                pass
            result_with_id = {
                **result,
                "task_id": task_id
            }
            insert_query = """
            INSERT INTO task_response (
                task_id,
                task_image_id,
                is_nsfw,
                clip_embeddings,
                image_hashes,
                result_from,
                result_memo,
                task_time_consumption,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            RETURNING id;
            """
            # Explicitly specify the order of values
            values = (
                result_with_id.get('task_id'),
                task_image_id,
                result_with_id.get('is_nsfw'),
                json.dumps(result_with_id.get('clip_embeddings')),
                json.dumps(result_with_id.get('image_hashes')),
                result_from,
                result_memo,
                json.dumps(task_time_consumption),
            )
            return await connection.fetchval(insert_query, *values)
    
    @with_db_retry_and_logging()
    async def insert_system_log(self, log_data):
        async with self.pool.acquire() as connection:
            insert_query = """
                INSERT INTO system_log (
                    host_ip,
                    post_endpoint,
                    vali_hk,
                    miner_hk,
                    queue,
                    worker_name,
                    function_name,
                    level,
                    message,
                    detail,
                    created_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP
                )
                RETURNING id;
            """

            values = (
                log_data.get('host_ip'),
                log_data.get('post_endpoint'),
                log_data.get('vali_hk'),
                log_data.get('miner_hk'),
                log_data.get('queue'),
                log_data.get('worker_name'),
                log_data.get('function_name'),
                log_data.get('level'),
                log_data.get('message'),
                log_data.get('detail'),
            )

            return await connection.fetchval(insert_query, *values)
    
    @with_db_retry_and_logging()
    async def get_random_text_task_payload_list(self, number, model):
        async with self.pool.acquire() as connection:
            select_query = """
            SELECT messages, temperature, seed, model, stream, logprobs, top_p, top_k, max_tokens, prompt FROM text_task
            WHERE result_from = 1"""
            if model:
                select_query += " AND model = $2"
            select_query += """
             ORDER BY RANDOM()
            LIMIT $1;
            """
            records = await connection.fetch(select_query, number, model)
            return [dict(record.items()) for record in records]

    @with_db_retry_and_logging()
    async def get_same_text_task_payload_list(self, number, model):
        async with self.pool.acquire() as connection:
            select_query = """
            SELECT messages, temperature, seed, model, stream, logprobs, top_p, top_k, max_tokens, prompt FROM text_task
            WHERE result_from = 1"""
            if model:
                select_query += " AND model = $1"
            select_query += """
             ORDER BY id DESC
            LIMIT 1;
            """
            records = await connection.fetch(select_query, model)
            return [dict(record.items()) for record in records] * number