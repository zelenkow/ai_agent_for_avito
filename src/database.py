import asyncpg
import json
from contextlib import asynccontextmanager
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

db_pool = None

async def create_db_pool():

    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            min_size=5,
            max_size=30,
            timeout=30
        )
        logger.info("Пул соединений БД создан")
    return db_pool

async def close_db_pool():

    global db_pool
    if db_pool:
        await db_pool.close()
        db_pool = None
        logger.info("Пул соединений БД закрыт")

@asynccontextmanager
async def get_connection():
    connection = await db_pool.acquire()
    try:
        yield connection
    finally:
        await db_pool.release(connection)        

async def get_chat_from_db():
    async with get_connection() as conn:

        query = "SELECT chat_id FROM chats;"
        chat_ids = await conn.fetch(query)
        chats_list = [record['chat_id'] for record in chat_ids]
        return chats_list
    
async def save_chats_to_db(mapped_chats):
    async with get_connection() as conn:
        inserted_count = 0

        query = """
            INSERT INTO chats (chat_id, title, client_name, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (chat_id)
            DO UPDATE SET
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.updated_at > chats.updated_at
            RETURNING xmax::text   
        """
    
        for chat in mapped_chats:
            result = await conn.fetchval(
                query,
                chat['chat_id'],
                chat['title'],
                chat['client_name'],
                chat['created_at'],
                chat['updated_at'],
            )
            if result == '0':
                inserted_count += 1

        logger.info(f"В БД добавлено: {inserted_count} чатов")
    
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
