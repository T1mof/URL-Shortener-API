import asyncio
import aiohttp


async def send_request(url, data):
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=data) as response:
            return await response.text()


async def main():
    url1 = 'http://127.0.0.1:5000/generate'
    # Данные для user_id = 1
    data1 = {
        "full_url": "https://ruz.spbstu.ru/faculty/125/groups/",
        "user_id": "1"
    }
    # Данные для user_id = 2
    data2 = {
        "full_url": "https://ruz.spbstu.ru/faculty/125/groups/",
        "user_id": "2"
    }
    # Создание задач для обоих пользователей
    tasks = [send_request(url1, data1) for _ in range(52)] + [send_request(url1, data2) for _ in range(52)]
    responses = await asyncio.gather(*tasks)
    for response in responses:
        print(response)


asyncio.run(main())
