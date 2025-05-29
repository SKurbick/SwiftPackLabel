import aiohttp
import asyncio
from src.db import db


API_URL = "https://content-api.wildberries.ru/content/v2/object/all"


async def fetch_and_store_data(api_key: str, locale: str = "ru", limit: int = 1000) -> None:
    """
    Асинхронно получает данные с API и сохраняет их в таблицы parent_data и subject_data.

    :param api_key: API ключ для авторизации
    :param locale: Язык данных (ru, en, zh)
    :param limit: Количество записей за запрос (максимум 1000)
    """
    offset = 0
    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                "locale": locale,
                "limit": limit,
                "offset": offset
            }
            headers = {
                "Authorization": api_key
            }
            async with session.get(API_URL, headers=headers, params=params, ssl=False) as response:
                if response.status != 200:
                    error_text = await response.text()
                    print(f"Ошибка запроса: {response.status} - {error_text}")
                    break

                json_data = await response.json()
                if json_data.get("error"):
                    print(f"Ошибка API: {json_data.get('errorText', 'Unknown error')}")
                    break

                data_batch = json_data.get("data", [])
                if not data_batch:
                    break


                for item in data_batch:
                    parent_id = item.get("parentID")
                    parent_name = item.get("parentName")
                    subject_id = item.get("subjectID")
                    subject_name = item.get("subjectName")

                    await db.execute("""
                        INSERT INTO parent_data (parent_wb_id, parent_name)
                        VALUES ($1, $2)
                        ON CONFLICT (parent_wb_id) DO NOTHING;
                    """, parent_id, parent_name)

                    await db.execute("""
                        INSERT INTO subject_data (subject_wb_id, subject_name, parent_id)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (subject_wb_id) DO NOTHING;
                    """, subject_id, subject_name, parent_id)

                if len(data_batch) < limit:
                    break

                offset += limit



if __name__ == "__main__":
    API_KEY = ""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_store_data(API_KEY, locale="ru"))
