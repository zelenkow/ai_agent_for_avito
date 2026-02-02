import os
import logging
from cachetools import TTLCache
import aiohttp
from dotenv import load_dotenv
from retry_config import api_retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
CLIENT_ID = os.getenv("AVITO_CLIENT_ID")
CLIENT_SECRET = os.getenv("AVITO_CLIENT_SECRET")

token_cache = TTLCache(maxsize=1, ttl=23.5 * 60 * 60)

@api_retry
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
            return new_token

@api_retry        
async def get_avito_chats(access_token, USER_ID):
    headers =  {'Authorization': f'Bearer {access_token}'}
    params = {'limit': 100,'offset': 0}
    url = f"https://api.avito.ru/messenger/v2/accounts/{USER_ID}/chats"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            raw_chats = await response.json()
            return raw_chats           

@api_retry        
async def get_avito_messages(access_token, chat_id, USER_ID):
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'limit': 100, 'offset': 0}
    url = f"https://api.avito.ru/messenger/v3/accounts/{USER_ID}/chats/{chat_id}/messages"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:

            if response.status != 200:
                logger.error(f"HTTP {response.status} для чата {chat_id}")
                return {"messages": []}
            
            raw_messages = await response.json()
            return raw_messages 