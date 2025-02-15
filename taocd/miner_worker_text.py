import json, hashlib, time, traceback, asyncio
from datetime import timedelta, timezone, datetime as dt
import httpx
from typing import Optional, Union

from taocd.db_manager import HOST_IP
from taocd.utils.rabbitmq import get_rabbitmq_client

from taocd.utils.logger import TaocdLogger
logger = TaocdLogger(log_file_prefix='miner_worker_text').get_logger()

from taocd.utils.text_task_queue import TextTaskQueue
text_task_queue = TextTaskQueue()

FINISH_TEXT = 'data: [DONE]\n\n'

def get_queue() -> Optional[str]:
    return None

def get_dict_md5(d: dict) -> str:
    data_string = json.dumps(d, sort_keys=True)
    md5_hash = hashlib.md5(data_string.encode('utf-8')).hexdigest()
    return md5_hash

async def get_chat_stream(self, body_dict, client_ip, path, hotkeys: dict = {
    "vali_hk": 'validator_hk_for_test',
    "miner_hk": 'miner_hk_for_test',
}, received_time: float = 0):
    queue = get_queue()
    task_md5 = get_dict_md5(body_dict)
    worker_name = ''
    raw_response = []
    final_response = []
    first_chunk_time = 0
    is_chat_request = "chat" in path
    try:
        worker_data = text_task_queue.add_to_least_busy_worker(model_name, task_md5, decrypted_payload.max_tokens, is_chat_request)
        if worker_data is None:
            raise Exception("No worker available")
        # 获取请求地址
        address = worker_data.get('address')
        # 获取队列名
        queue = worker_data.get('queue_name')
        # 获取服务名
        worker_name = worker_data.get('worker_name')
    except Exception as e:
        text_task_queue.remove(model_name, worker_name, task_md5)
        result_memo = f"{e}"
        asyncio.create_task(insert_text_task_log(hotkeys=hotkeys, decrypted_payload_dict=decrypted_payload_dict, queue=queue, task_md5=task_md5, worker_name=worker_name, client_ip=client_ip, received_time=received_time, first_chunk_time=first_chunk_time, exec_time=time.time() - received_time, host_ip=HOST_IP, raw_response=raw_response, final_response=final_response, result_from=0, result_memo=result_memo))
        raise
    async with self.client.stream("POST", path, json=body_dict, timeout=10) as resp:
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            text_task_queue.remove(model_name, worker_name, task_md5)
            await resp.aread()
            result_memo = f"HTTP Error {e.response.status_code}: {e.response.text}"
            logger.error(result_memo)
            asyncio.create_task(insert_text_task_log(hotkeys=hotkeys, path=path, body_dict=body_dict, queue=queue, task_md5=task_md5, worker_name=worker_name, client_ip=client_ip, received_time=received_time, first_chunk_time=first_chunk_time, exec_time=time.time() - received_time, host_ip=HOST_IP, raw_response=raw_response, final_response=final_response, result_from=0, result_memo=result_memo))
            raise
        try:
            async for chunk in resp.aiter_lines():
                raw_response.append(chunk)
                received_event_chunks = chunk.split("\n\n")
                for event in received_event_chunks:
                    if event == "":
                        continue
                    prefix, _, data = event.partition(":")
                    if data.strip() == "[DONE]":
                        break
                    # This is quite ineffecient but needed
                    # To work with base vllm image
                    # I would recommended optimising this in some way
                    # print(data)
                    data2 = json.loads(data)
                    try:
                        if (
                            is_chat_request and
                            (not data2["choices"]
                            or data2["choices"][0]["logprobs"] is None
                            or "content" not in data2["choices"][0]["logprobs"]
                            or not data2["choices"][0]["logprobs"]["content"]
                            or data2["choices"][0]["logprobs"]["content"][0]["logprob"] is None)
                        ):
                            continue
                        elif (
                            (not is_chat_request) and
                            (not data2["choices"]
                            or data2["choices"][0]["logprobs"] is None
                            or data2["choices"][0]["logprobs"]["token_logprobs"] is None)
                        ):
                            continue
                    except Exception as e:
                        logger.error(f"Error in judge: {e}")
                        continue

                    final_response.append(f"data: {data}\n\n")
                    if first_chunk_time == 0:
                        first_chunk_time = time.time() - received_time
                    yield f"data: {data}\n\n"
        except Exception as e:
            logger.error(f"Error during streaming: {e}")
            raise
        finally: 
            text_task_queue.remove(model_name, worker_name, task_md5)
            final_response.append(FINISH_TEXT)
            asyncio.create_task(insert_text_task_log(hotkeys=hotkeys, decrypted_payload_dict=decrypted_payload_dict, queue=queue, task_md5=task_md5, worker_name=worker_name, client_ip=client_ip, received_time=received_time, first_chunk_time=first_chunk_time, exec_time=time.time() - received_time, host_ip=HOST_IP, raw_response=raw_response, final_response=final_response, result_from=1, result_memo=''))
            yield FINISH_TEXT

async def insert_text_task_log(hotkeys, path, body_dict, queue, task_md5, worker_name, client_ip, received_time, first_chunk_time, exec_time, host_ip, raw_response, final_response, result_from, result_memo):
    try:
        is_chat_request = "chat" in path
        rabbitmq_client = await get_rabbitmq_client()
        data = {
            "vali_hk": hotkeys.get('vali_hk'),
            "miner_hk": hotkeys.get('miner_hk'),
            "path": path,
            "messages": body_dict.get('messages'),
            "prompt": body_dict.get('prompt'),
            "temperature": body_dict.get('temperature'),
            "seed": body_dict.get('seed'),
            "model": body_dict.get('model'),
            "stream": body_dict.get('stream'),
            "logprobs": body_dict.get('logprobs'),
            "top_p": body_dict.get('top_p'),
            "top_k": body_dict.get('top_k'),
            "max_tokens": body_dict.get('max_tokens'),
            "queue": queue,
            "task_md5": task_md5,
            "worker_name": worker_name,
            "client_ip": client_ip,
            "received_time": dt.fromtimestamp(received_time).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "first_chunk_time": first_chunk_time,
            "exec_time": exec_time,
            "host_ip": host_ip,
            "raw_response": json.dumps(raw_response),
            "final_response": json.dumps(final_response),
            "result_from": result_from,
            "result_memo": result_memo
        }
        data_str = json.dumps(data)
        await rabbitmq_client.send_message(message=data_str)
    except Exception as error:
        logger.error(f"insert_text_task_log error: {error}")

def test():
    pass

if __name__ == '__main__':
    test()