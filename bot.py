import os
import logging
import aiohttp
import asyncpg
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

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

bot = Bot(token=TOKEN)
dp = Dispatcher()

token_cache = TTLCache(maxsize=1, ttl=23.5 * 60 * 60)

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
    connection_string = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    conn = await asyncpg.connect(connection_string)

    try:
        query = "SELECT chat_id FROM chats;"
        chat_ids = await conn.fetch(query)
        chats_list = [record['chat_id'] for record in chat_ids]
        logger.info(f"Из БД получено {len(chats_list)} chat_id для обработки.")
        return chats_list
    finally:
        await conn.close()

async def save_chats_to_db(mapped_chats):
    connection_string = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    conn = await asyncpg.connect(connection_string)

    try:
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
    
    finally:
        await conn.close()

async def save_messages_to_db(messages_list):
    connection_string = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    conn = await asyncpg.connect(connection_string)
    
    try:
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

    finally:
        await conn.close()
            
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

@dp.message(Command("report"))
async def start(message: types.Message):
    token = await get_avito_token()
    await all_doing_for_chats(token)
    await all_doing_for_messages(token)

@dp.message()
async def send_way(message: types.Message):
    await message.answer("Don't Do It")

if __name__ == "__main__":
    dp.run_polling(bot)