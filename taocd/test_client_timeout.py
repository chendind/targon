import httpx
import asyncio

async def main():
    url = "http://127.0.0.1:40001/v1/chat/completions_test"

    payload = {
        "model": "NousResearch/Meta-Llama-3.1-8B-Instruct",
        "messages": [
            {
                "role": "user",
                "content": "请写一篇1000字的文章，主题是关于人工智能的未来发展。"
            }
        ],
        "temperature": 1,
        "seed": 1,
        "stream": True,
        "logprobs": True,
        "top_p": 1,
        "top_k": 5,
        "max_tokens": 4000
    }
    headers = {
        "Authorization": "Bearer sk-DtT3YuhbLJ8NLc7n730cFbA584794b5890C30e173859A00e",
        "Content-Type": "application/json"
    }

    timeout = 2  # 设置请求超时时间为10秒

    async with httpx.AsyncClient() as client:
        async with client.stream(
            method="POST",
            url=url,
            json=payload,
            headers=headers,
            timeout=timeout,
        ) as response:
            try:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    print(line)
            except httpx.HTTPStatusError as e:
                await response.aread()
                print(f"HTTP Error {e.response.status_code}: {e.response.text}")
                raise
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                raise

if __name__ == '__main__':
    asyncio.run(main())
