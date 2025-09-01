import os
import logging
import aiohttp
import asyncpg
import json
from contextlib import asynccontextmanager
from datetime import datetime 
from cachetools import TTLCache
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CLIENT_ID = os.getenv("AVITO_CLIENT_ID")
CLIENT_SECRET = os.getenv("AVITO_CLIENT_SECRET")
DIKON_ID = os.getenv("DIKON_USER_ID")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

bot = Bot(token=TOKEN)
dp = Dispatcher()
db_pool = None
token_cache = TTLCache(maxsize=1, ttl=23.5 * 60 * 60)

async def create_db_pool():
    return await asyncpg.create_pool(
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        min_size=5,
        max_size=20,
        timeout=30
    )

async def on_startup():
    global db_pool
    logger.info("Создаем пул соединений с БД")
    db_pool = await create_db_pool()
    logger.info("Пул соединений создан")

async def on_shutdown():
    global db_pool
    if db_pool:
        logger.info("Закрываем пул соединений с БД")
        await db_pool.close()
        logger.info("Пул соединений закрыт")    

async def get_avito_token():
    if 'avito_token' in token_cache:
        logger.info("Используется кешированный токен")
        return token_cache['avito_token']
    
    logger.info("Запрашивается новый токен")

    data_api = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.avito.ru/token",
            data=data_api,
        ) as response:
            token_data = await response.json()
            new_token = token_data["access_token"]
            token_cache['avito_token'] = new_token

            logger.info("Функция get_avito_token завершена успешно")
            return new_token

async def all_doing_for_chats(access_token):
    raw_data_chats = await get_avito_chats(access_token)

    map_data_chats = map_avito_chats(raw_data_chats, DIKON_ID)

    await save_chats_to_db(map_data_chats)
    logger.info("Функция all_doing_for_chats завершена успешно")

async def all_doing_for_messages(access_token):
    all_messages_to_save = []

    chats_list = await get_chat_from_db()

    for chat_id in chats_list:
        raw_messages = await get_avito_messages(access_token, chat_id)
        mapped_messages = map_avito_messages(raw_messages, chat_id)
        all_messages_to_save.extend(mapped_messages)

    await save_messages_to_db(all_messages_to_save)
    logger.info("Функция all_doing_for_messages завершена успешно")

async def get_avito_chats(access_token):
    headers =  {'Authorization': f'Bearer {access_token}'}
    params = {'limit': 100,'offset': 0}
    url = f"https://api.avito.ru/messenger/v2/accounts/{DIKON_ID}/chats"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            raw_chats = await response.json()
            return raw_chats

async def get_avito_messages(access_token, chat_id):
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'limit': 100, 'offset': 0}
    url = f"https://api.avito.ru/messenger/v3/accounts/{DIKON_ID}/chats/{chat_id}/messages"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            raw_messages = await response.json()
            return raw_messages             

async def get_chat_from_db():
    async with get_connection() as conn:

        query = "SELECT chat_id FROM chats;"
        chat_ids = await conn.fetch(query)
        chats_list = [record['chat_id'] for record in chat_ids]
        logger.info(f"Из БД получено {len(chats_list)} chat_id для обработки.")
        return chats_list

async def save_chats_to_db(mapped_chats):
    async with get_connection() as conn:

        upsert_query = """
            INSERT INTO chats (chat_id, title, client_name, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (chat_id)
            DO UPDATE SET
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.updated_at > chats.updated_at    
        """
    
        for chat in mapped_chats:
            await conn.execute(
                upsert_query,
                chat['chat_id'],
                chat['title'],
                chat['client_name'],
                chat['created_at'],
                chat['updated_at'],
            )
        
        logger.info(f"Успешно сохранено {len(mapped_chats)} чатов")

        return True

async def save_messages_to_db(messages_list):
    async with get_connection() as conn:
    
        query = """
            INSERT INTO messages 
                (message_id, chat_id, text, is_from_company, created_at)
            VALUES 
                ($1, $2, $3, $4, $5)
            ON CONFLICT (message_id) 
            DO NOTHING
        """
        
        records = []
        for msg in messages_list:
            records.append((
                msg['message_id'],
                msg['chat_id'],
                msg['text'],
                msg['is_from_company'],
                msg['created_at'],
            ))
        
        await conn.executemany(query, records)

        logger.info(f"Успешно сохранено {len(messages_list)} сообщений в БД")

        return True

async def get_chats_for_analysis():
    async with get_connection() as conn:

        query = """
            SELECT 
                chats.chat_id
            FROM 
                chats
            LEFT JOIN 
                chat_reports ON chats.chat_id = chat_reports.chat_id
            WHERE 
                chat_reports.chat_id IS NULL 
                OR 
                chats.updated_at > chat_reports.created_at
            ORDER BY 
                chats.updated_at DESC;
        """

        records = await conn.fetch(query)

        chat_ids_for_analysis = []
        for record in records:
            chat_id = record['chat_id']
            chat_ids_for_analysis.append(chat_id)

        logger.info(f"Найдено чатов для анализа: {len(chat_ids_for_analysis)}")
        return chat_ids_for_analysis

async def get_chat_data_for_analysis(chat_id):
    async with get_connection() as conn:

        query = """
            SELECT 
                chats.chat_id,
                chats.title,
                json_agg(
                    json_build_object(
                        'text', messages.text,
                        'is_from_company', messages.is_from_company,
                        'created_at', messages.created_at
                    ) ORDER BY messages.created_at ASC
                ) as messages
            FROM chats
            LEFT JOIN messages ON chats.chat_id = messages.chat_id
            WHERE chats.chat_id = $1
            GROUP BY chats.chat_id, chats.title
        """
        record = await conn.fetchrow(query, chat_id)

        messages = json.loads(record['messages']) if record['messages'] else []

        chat_data = {
            'chat_id': record['chat_id'],
            'chat_title': record['title'],
            'messages': messages
        }
        
        return chat_data

async def send_to_deepseek(prompt_data):
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": prompt_data["system"]},
            {"role": "user", "content": prompt_data["user"]}
        ],
        "temperature": 0.1,
        "response_format": { "type": "json_object" }
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.deepseek.com/v1/chat/completions", 
            headers=headers, 
            json=payload,
            timeout=60
        ) as response:
            
            result = await response.json()
            return result
            
def map_avito_chats(raw_chats_data, my_user_id):
    mapped_chats = []
    
    for chat in raw_chats_data.get('chats', []):
        client_name = 'Неизвестный клиент'
        for user in chat.get('users', []):
            if user.get('id') != my_user_id and user.get('name'):
                client_name = user['name']
                break

        mapped_chat = {
            'chat_id': chat.get('id', ''),
            'title': chat.get('context', {}).get('value', {}).get('title', ''),
            'client_name': client_name,
            'created_at': datetime.fromtimestamp(chat.get('created', 0)),
            'updated_at': datetime.fromtimestamp(chat.get('updated', 0))
        }
        mapped_chats.append(mapped_chat)

    return mapped_chats

def map_avito_messages(raw_messages_data, chat_id):
    mapped_messages = []

    for message in raw_messages_data.get('messages', []):
        if message.get('type') == 'system':
            continue

        direction = message.get('direction', '')
        is_from_company = (direction == 'out')

        mapped_message = {
            'chat_id': chat_id,
            'message_id': message.get('id', ''),
            'text': message.get('content', {}).get('text', ''),
            'is_from_company': is_from_company,
            'created_at': datetime.fromtimestamp(message.get('created', 0)),
        }
        mapped_messages.append(mapped_message)

    return mapped_messages

def create_prompt(chat_data):
    messages = chat_data['messages']
    formatted_lines = []
    for msg in messages:
        role = "[МЕНЕДЖЕР]" if msg['is_from_company'] else "[КЛИЕНТ]"
        message_text = msg['text']
        formatted_lines.append(f"{role}\n- {message_text}")
    
    formatted_dialog = "\n\n".join(formatted_lines)
    
    system_prompt = """
Ты — AI-ассистент для контроля качества коммуникации менеджеров в компании.
Твоя задача — строго проанализировать диалог и вернуть ответ в формате JSON, без любых других пояснений до или после.
ВСЕГДА следуй предложенной схеме JSON.
ВСЕ части ответа, включая комментарии и рекомендации, ДОЛЖНЫ быть написаны на РУССКОМ ЯЗЫКЕ.
ЗАПРЕЩЕНО использовать английские слова и термины.
""".strip()
    
    user_prompt = f"""
Проанализируй диалог менеджера с клиентом в чате "{chat_data['chat_title']}".
Учти, что [КЛИЕНТ] — это потенциальный покупатель, а [МЕНЕДЖЕР] — это сотрудник компании.

Сообщения от КОМПАНИИ помечены [МЕНЕДЖЕР], от КЛИЕНТА - [КЛИЕНТ].

ПРОАНАЛИЗИРУЙ СООБЩЕНИЯ [МЕНЕДЖЕР] и дай развернутую оценку по следующим критериям. Для каждого критерия дай ОБЩУЮ ОЦЕНКУ ("Высокая", "Средняя", "Низкая") и КРАТКОЕ ПОЯСНЕНИЕ на 1-2 предложения на русском языке.

КРИТЕРИИ:
1.  **Тональность коммуникации**: Общий эмоциональный настрой и вежливость.
2.  **Профессионализм**: Использование корректной терминологии, компетентность в вопросах.
3.  **Ясность изложения**: Насколько понятно, четко и структурировано менеджер доносит информацию.
4.  **Решение проблем**: Способность выявлять потребности клиента и предлагать релевантные решения.
5.  **Работа с возражениями**: Эффективность реакции на сомнения или негатив клиента. Если возражений не было, поставь оценку 'Нет возражений'.
6.  **Завершение диалога**: Была ли сделана попытка корректно завершить коммуникацию (зафиксировать следующий шаг, попрощаться).

ВСЕ оценки, комментарии и рекомендации ДОЛЖНЫ БЫТЬ НАПИСАНЫ НА РУССКОМ ЯЗЫКЕ. ЗАПРЕЩЕНО использовать английские слова, заменяй их русскими аналогами.

В конце дай:
- **Итоговую оценку**: Краткое резюме на 1-3 предложения на русском языке.
- **Рекомендации**: 1-3 конкретных совета, что менеджер мог бы сделать лучше на русском языке

ВЕРНИ ОТВЕТ В ФОРМАТЕ JSON СТРОГО И ТОЧНО ПО СЛЕДУЮЩЕЙ СХЕМЕ. НЕ ДОБАВЛЯЙ никаких других полей.

{{
  "tonality": {{
    "grade": "Высокая",
    "comment": "Менеджер сохранял доброжелательный и уважительный тон на протяжении всего диалога."
  }},
  "professionalism": {{
    "grade": "Средняя", 
    "comment": "Использовал корректную терминологию, но не уточнил важные технические детали по установке."
  }},
  "clarity": {{
    "grade": "Высокая",
    "comment": "Ответы были четкими и по делу, клиенту было легко понять варианты и цены."
  }},
  "problem_solving": {{
    "grade": "Низкая",
    "comment": "Не предложил альтернативу при отказе клиента от дорогого варианта."
  }},
  "objection_handling": {{
    "grade": "Нет возражений",
    "comment": "В диалоге возражений со стороны клиента не было."
  }},
  "closure": {{
    "grade": "Высокая",
    "comment": "Диалог завершен корректно, клиент приглашен для дальнейшего обращения."
  }},
  "summary": "Менеджер вежлив и коммуникабелен, но не проявил гибкости в продажах. Клиент ушел на подумать без конкретного решения.",
  "recommendations": "Отработать технику предложения альтернатив. Заранее готовить ответы на частые возражения по цене."
}}

ДИАЛОГ:
{formatted_dialog}
""".strip()
    
    return {
        "system": system_prompt,
        "user": user_prompt
    }

@asynccontextmanager
async def get_connection():
    connection = await db_pool.acquire()
    try:
        yield connection
    finally:
        await db_pool.release(connection) 

@dp.message(Command("report"))
async def start(message: types.Message):
    token = await get_avito_token()
    await all_doing_for_chats(token)
    await all_doing_for_messages(token)

@dp.message(Command("start"))
async def start(message: types.Message):
    chats_for_analysis = await get_chats_for_analysis()
    first_chat_id = chats_for_analysis[1]
    chat_data = await get_chat_data_for_analysis(first_chat_id)
    prompt_data = create_prompt(chat_data)
    response = await send_to_deepseek(prompt_data)

    beautiful_json = response['choices'][0]['message']['content']
        
    with open("deepseek_response.json", "w", encoding="utf-8") as f:
        f.write(beautiful_json)
            
@dp.message()
async def send_way(message: types.Message):
    await message.answer("Don't Do It")

if __name__ == "__main__":

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    dp.run_polling(bot)