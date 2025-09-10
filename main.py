import os
import logging
import aiohttp
import asyncpg
import json
import asyncio
import argparse
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from cachetools import TTLCache
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import ReplyKeyboardBuilder

class ReportState(StatesGroup):
    waiting_for_start_date = State()
    waiting_for_end_date = State()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
CLIENT_TELEGRAM_IDS = [int(x) for x in os.getenv('CLIENT_TELEGRAM_IDS', '').split(',') if x]

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

def map_avito_chats(raw_chats_data, my_user_id):
    mapped_chats = []
    
    for chat in raw_chats_data.get('chats', []):
        client_name = ''
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

def map_response_llm (response, chat_id, chat_data):
    chat_title = chat_data.get('chat_title', '')
    client_name = chat_data.get('chat_client_name', '')
    chat_created_at = chat_data.get('chat_created_at', '')
    chat_updated_at = chat_data.get('chat_updated_at', '')
    total_messages = chat_data.get('total_messages', 0)
    company_messages = chat_data.get('company_messages', 0)
    client_messages = chat_data.get('client_messages', 0)
    
    mapped_data = {
        'chat_id': chat_id,
        'created_at': datetime.now(),
        'chat_title': chat_title,
        'client_name': client_name,
        'chat_created_at': chat_created_at,
        'chat_updated_at': chat_updated_at,
        'total_messages': total_messages,
        'company_messages': company_messages,
        'client_messages': client_messages,
        'tonality_grade': response.get('tonality', {}).get('grade', ''),
        'tonality_comment': response.get('tonality', {}).get('comment', ''),
        'professionalism_grade': response.get('professionalism', {}).get('grade', ''),
        'professionalism_comment': response.get('professionalism', {}).get('comment', ''),
        'clarity_grade': response.get('clarity', {}).get('grade', ''),
        'clarity_comment': response.get('clarity', {}).get('comment', ''),
        'problem_solving_grade': response.get('problem_solving', {}).get('grade', ''),
        'problem_solving_comment': response.get('problem_solving', {}).get('comment', ''),
        'objection_handling_grade': response.get('objection_handling', {}).get('grade', ''),
        'objection_handling_comment': response.get('objection_handling', {}).get('comment', ''),
        'closure_grade': response.get('closure', {}).get('grade', ''),
        'closure_comment': response.get('closure', {}).get('comment', ''),
        'summary': response.get('summary', ''),
        'recommendations': response.get('recommendations', '')
    }
    return mapped_data

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

def format_single_report(report_data):
    
    grades_text = ""
    criteria = [
        ("Тональность", "tonality_grade", "tonality_comment"),
        ("Профессионализм", "professionalism_grade", "professionalism_comment"),
        ("Ясность", "clarity_grade", "clarity_comment"),
        ("Решение проблем", "problem_solving_grade", "problem_solving_comment"),
        ("Работа с возражениями", "objection_handling_grade", "objection_handling_comment"),
        ("Завершение", "closure_grade", "closure_comment")
    ]
    for name, grade_key, comment_key in criteria:
        grade = report_data.get(grade_key, '')
        comment = report_data.get(comment_key, '')
        grades_text += f"• <b>{name}:</b> {grade}\n"
        grades_text += f"  <i>{comment}</i>\n\n"
    return f"""

<b>Чат по обьявлению:</b> {report_data.get('chat_title', '')}
<b>Клиент:</b> {report_data.get('client_name', '')}
<b>Дата создания:</b> {report_data['chat_created_at'].strftime('%d.%m.%Y') if report_data.get('chat_created_at') else ''}
<b>Дата последнего сообщения:</b> {report_data['chat_updated_at'].strftime('%d.%m.%Y') if report_data.get('chat_updated_at') else ''}

<b>Общее количество сообщений:</b> {report_data.get('total_messages', 0)}
<b>Сообщений от менеджера:</b> {report_data.get('company_messages', 0)}
<b>Сообщений от клиента:</b> {report_data.get('client_messages', 0)}
<b>Дата анализа:</b> {report_data['created_at'].strftime('%d.%m.%Y') if report_data.get('created_at') else ''}
  
<b>Оценка ИИ:</b>

{grades_text}
<b>Итог:</b>
<i>{report_data.get('summary', '')}</i>

<b>Рекомендации:</b>
<i>{report_data.get('recommendations', '')}</i>
"""

def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text="Отчет за период"))
    return builder.as_markup(resize_keyboard=True)

async def send_reports_on_timer():
    try:    
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59)
        
        reports = await get_reports_from_db(start_date, end_date)
        
        for client_chat_id in CLIENT_TELEGRAM_IDS:
                
                await bot.send_message(
                    chat_id=client_chat_id,
                    text=f"<b>Ежедневный отчет за {yesterday.strftime('%d.%m.%Y')}</b>\n\n"
                         f"Всего отчетов: {len(reports)}",
                    parse_mode='HTML'
                )   
                for report in reports:
                    report_text = format_single_report(report)
                    await bot.send_message(
                        chat_id=client_chat_id,
                        text=report_text,
                        parse_mode='HTML'
                    )
                    await asyncio.sleep(2.0)

    except Exception as e:
        logger.error(f"Ошибка в функции send_reports_on_timer: {e}")
        return False                       
                
async def main_avito_data():
    try:
        token = await get_avito_token()
        raw_data_chats = await get_avito_chats(token)
        map_data_chats = map_avito_chats(raw_data_chats, DIKON_ID)
        await save_chats_to_db(map_data_chats)
    
        all_messages_to_save = []
        chats_list = await get_chat_from_db()

        logger.info("Чаты получены, начинаю синхронизацию сообщений...")
        
        for chat_id in chats_list:
            raw_messages = await get_avito_messages(token, chat_id)
            mapped_messages = map_avito_messages(raw_messages, chat_id)
            all_messages_to_save.extend(mapped_messages)
        
        await save_messages_to_db(all_messages_to_save)
        logger.info("Cинхронизация данных с Авито завершена успешно")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка функции main_avito_data: {e}")
        return False
        
async def main_llm_data():
    try:
        logger.info("Получение чатов для анализа")
        chat_ids = await get_chats_for_analysis()

        if not chat_ids:
            logger.info("Нет новых чатов для анализа.")
            return True
        logger.info("Чаты получены, начинаю анализ...")
        chat_ids = chat_ids[:3]
        semaphore = asyncio.Semaphore(10)

        async def process_chat(chat_id):
            async with semaphore:
                try:
                    chat_data = await get_chat_data_for_analysis(chat_id)
                    prompt_data = create_prompt(chat_data)
                    analysis_result = await send_to_deepseek(prompt_data)
                    mapped_data = map_response_llm(analysis_result, chat_id, chat_data)
                    await save_reports_to_db(mapped_data)
                    return True
                except Exception as e:
                    logger.error(f"Ошибка при обработке чата {chat_id}: {e}")
                    raise e
    
        tasks = [process_chat(chat_id) for chat_id in chat_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка задачи: {result}")
    
        logger.info(f"Анализ {len(chat_ids)} чатов завершен")
        return True
    except Exception as e:
        logger.error(f"Ошибка функции main_llm_data: {e}")
        return False

async def create_db_pool():
    return await asyncpg.create_pool(
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        min_size=5,
        max_size=30,
        timeout=30
    )

async def on_startup():
    global db_pool
    db_pool = await create_db_pool()
    logger.info("Пул соединений БД открыт")

async def on_shutdown():
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("Пул соединений БД закрыт")   

    if hasattr(bot, 'session') and bot.session:
        await bot.session.close()     

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
        return chats_list

async def save_chats_to_db(mapped_chats):
    async with get_connection() as conn:

        query = """
            INSERT INTO chats (chat_id, title, client_name, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (chat_id)
            DO UPDATE SET
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.updated_at > chats.updated_at    
        """
    
        for chat in mapped_chats:
            await conn.execute(
                query,
                chat['chat_id'],
                chat['title'],
                chat['client_name'],
                chat['created_at'],
                chat['updated_at'],
            )

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

        return True
    
async def save_reports_to_db(mapped_data):
         async with get_connection() as conn:
             
            query = """
                INSERT INTO chat_reports
                    (chat_id, created_at, chat_title, client_name,  chat_created_at, chat_updated_at,
                    total_messages, company_messages, client_messages, tonality_grade, tonality_comment, 
                    professionalism_grade, professionalism_comment, clarity_grade, clarity_comment, 
                    problem_solving_grade, problem_solving_comment, objection_handling_grade, 
                    objection_handling_comment, closure_grade, closure_comment, summary, recommendations)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    chat_title = EXCLUDED.chat_title,
                    created_at = EXCLUDED.created_at,
                    client_name = EXCLUDED.client_name,
                    chat_created_at = EXCLUDED.chat_created_at,
                    chat_updated_at = EXCLUDED.chat_updated_at,
                    total_messages = EXCLUDED.total_messages,
                    company_messages = EXCLUDED.company_messages,
                    client_messages = EXCLUDED.client_messages,
                    tonality_grade = EXCLUDED.tonality_grade,
                    tonality_comment = EXCLUDED.tonality_comment,
                    professionalism_grade = EXCLUDED.professionalism_grade,
                    professionalism_comment = EXCLUDED.professionalism_comment,
                    clarity_grade = EXCLUDED.clarity_grade,
                    clarity_comment = EXCLUDED.clarity_comment,
                    problem_solving_grade = EXCLUDED.problem_solving_grade,
                    problem_solving_comment = EXCLUDED.problem_solving_comment,
                    objection_handling_grade = EXCLUDED.objection_handling_grade,
                    objection_handling_comment = EXCLUDED.objection_handling_comment,
                    closure_grade = EXCLUDED.closure_grade,
                    closure_comment = EXCLUDED.closure_comment,
                    summary = EXCLUDED.summary,
                    recommendations = EXCLUDED.recommendations
                WHERE EXCLUDED.created_at > chat_reports.created_at
            """

            await conn.execute(
                query,
                mapped_data['chat_id'],
                mapped_data['created_at'],
                mapped_data['chat_title'],
                mapped_data['client_name'],
                mapped_data['chat_created_at'],
                mapped_data['chat_updated_at'],
                mapped_data['total_messages'],
                mapped_data['company_messages'],
                mapped_data['client_messages'],
                mapped_data['tonality_grade'],
                mapped_data['tonality_comment'],
                mapped_data['professionalism_grade'],
                mapped_data['professionalism_comment'],
                mapped_data['clarity_grade'],
                mapped_data['clarity_comment'],
                mapped_data['problem_solving_grade'],
                mapped_data['problem_solving_comment'],
                mapped_data['objection_handling_grade'],
                mapped_data['objection_handling_comment'],
                mapped_data['closure_grade'],
                mapped_data['closure_comment'],
                mapped_data['summary'],
                mapped_data['recommendations']
            )

            return True
         
async def get_reports_from_db(start_date, end_date):
    async with get_connection() as conn:
       
            query = """
                SELECT * FROM chat_reports 
                WHERE created_at BETWEEN $1 AND $2 
                ORDER BY created_at DESC
            """

            records = await conn.fetch(query, start_date, end_date)

            reports = []
            for record in records:
                reports.append(dict(record))
                
            return reports
                   
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

        return chat_ids_for_analysis

async def get_chat_data_for_analysis(chat_id):
    async with get_connection() as conn:

        query = """
            SELECT 
                chats.chat_id,
                chats.title,
                chats.client_name,
                chats.created_at,
                chats.updated_at,
                json_agg(
                    json_build_object(
                        'text', messages.text,
                        'is_from_company', messages.is_from_company,
                        'created_at', messages.created_at
                    ) ORDER BY messages.created_at ASC
                ) as messages,
            COUNT(messages.message_id) as total_messages,
            COUNT(messages.message_id) FILTER (WHERE messages.is_from_company = true) as company_messages,
            COUNT(messages.message_id) FILTER (WHERE messages.is_from_company = false) as client_messages    
            FROM chats
            LEFT JOIN messages ON chats.chat_id = messages.chat_id
            WHERE chats.chat_id = $1
            GROUP BY chats.chat_id, chats.title, chats.client_name, chats.created_at, chats.updated_at
        """
        record = await conn.fetchrow(query, chat_id)

        messages = json.loads(record['messages']) if record['messages'] else []

        chat_data = {
            'chat_id': record['chat_id'],
            'chat_title': record['title'],
            'chat_client_name': record['client_name'],
            'chat_created_at': record['created_at'],
            'chat_updated_at': record['updated_at'],
            'messages': messages,
            'total_messages': record['total_messages'] or 0,
            'company_messages': record['company_messages'] or 0,
            'client_messages': record['client_messages'] or 0
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
            content_json = result['choices'][0]['message']['content']
            return json.loads(content_json)
          
@asynccontextmanager
async def get_connection():
    connection = await db_pool.acquire()
    try:
        yield connection
    finally:
        await db_pool.release(connection) 

@dp.message(Command("myid"))
async def cmd_myid(message: types.Message):
    await message.answer(f"Ваш ID: {message.chat.id}")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "Добро пожаловать! Я бот для анализа диалогов Авито",
        reply_markup=get_main_keyboard()
    )

@dp.message(lambda message: message.text == "Отчет за период")
async def cmd_report(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите начальную дату периода\n\n"
        "Пример: 01.09.2025",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(ReportState.waiting_for_start_date)

@dp.message(ReportState.waiting_for_start_date)
async def process_start_date(message: types.Message, state: FSMContext):
    try:
        if not message.text or not isinstance(message.text, str):
            await message.answer("Неверный формат даты, попробуйте еще раз:")
            return
        try:
            start_date = datetime.strptime(message.text, '%d.%m.%Y')
            await state.update_data(start_date=start_date)
            await message.answer(
                "Введите конечную дату периода\n\n"
                "Пример: 15.09.2025",
            )
            await state.set_state(ReportState.waiting_for_end_date)
        except ValueError:
            await message.answer("Неверный формат даты, попробуйте еще раз:")

    except Exception as e:
        logger.error(f"Ошибка функции process_start_data: {e}")
        await message.answer("Произошла внутренняя ошибка. Попробуйте снова.")
                
@dp.message(ReportState.waiting_for_end_date)
async def process_end_date(message: types.Message, state: FSMContext):
    try:
        if not message.text or not isinstance(message.text, str):
            await message.answer("Неверный формат даты, попробуйте еще раз:")
            return
        try:
            end_date_input = datetime.strptime(message.text, '%d.%m.%Y')
            end_date = end_date_input.replace(hour=23, minute=59, second=59)
            data = await state.get_data()
            start_date = data['start_date']
            await state.clear()
            reports = await get_reports_from_db(start_date, end_date)

            if not reports:
                await message.answer("Отчеты за указанный период отсутствуют")
                return

            for report in reports:
                report_text = format_single_report(report)
                await message.answer(report_text, parse_mode='HTML')
                await asyncio.sleep(2.0)

            await message.answer("Отчеты за указанный период сформированы!")

        except ValueError:
            await message.answer("Неверный формат даты, попробуйте еще раз:")

    except Exception as e:
        logger.error(f"Ошибка функции process_end_date: {e}")
        await message.answer("Произошла внутренняя ошибка. Попробуйте снова.")        

@dp.message()
async def block_all_messages(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    
    if current_state is None:
        await message.answer(
            "<b>Ввод недоступен</b>\n\n"
            "Пожалуйста, используйте кнопку ниже:",
            parse_mode='HTML',
            reply_markup=get_main_keyboard()
        )

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--command')
    args = parser.parse_args()

    if args.command in ['avito', 'llm', 'timer']:
        async def main():
            await on_startup()
            
            if args.command == 'avito':
                await main_avito_data()

            elif args.command == 'llm':
                await main_llm_data()

            elif args.command == 'timer':
                await send_reports_on_timer()    
                
            await on_shutdown()
        
        asyncio.run(main())
    
    else:
        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)
        dp.run_polling(bot)