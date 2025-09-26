import os
import logging
import argparse
import asyncio
import random
import database
import avito
import utils
import llm
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
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
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DIKON_ID = os.getenv("DIKON_USER_ID")

moscow_tz = timezone(timedelta(hours=3))
bot = Bot(token=TOKEN)
dp = Dispatcher()

def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text="Отчет за период"))
    return builder.as_markup(resize_keyboard=True)

def setup_scheduler():
    scheduler = AsyncIOScheduler(timezone=moscow_tz)
    scheduler.add_job(
        scheduled_avito_task,
        CronTrigger(hour=22, minute=0, timezone=moscow_tz),
    )
    scheduler.add_job(
        scheduled_llm_task,
        CronTrigger(hour=23, minute=0, timezone=moscow_tz),
    )
    scheduler.add_job(
        scheduled_reports_task,
        CronTrigger(hour=10, minute=0, timezone=moscow_tz),
    )
    return scheduler

async def scheduled_avito_task():
    await main_avito_data()

async def scheduled_llm_task():
    await main_llm_data()

async def scheduled_reports_task():
    await send_reports_on_timer()      
               
async def main_avito_data():
    try:
        token = await avito.get_avito_token()
        raw_data_chats = await avito.get_avito_chats(token, DIKON_ID)
        map_data_chats = utils.map_avito_chats(raw_data_chats, DIKON_ID)
        await database.save_chats_to_db(map_data_chats)
    
        all_messages_to_save = []
        chats_list = await database.get_chat_from_db()

        logger.info("Чаты получены, начинаю синхронизацию сообщений...")
        
        for chat_id in chats_list:
            raw_messages = await avito.get_avito_messages(token, chat_id, DIKON_ID)
            mapped_messages = utils.map_avito_messages(raw_messages, chat_id)
            all_messages_to_save.extend(mapped_messages)
        
        await database.save_messages_to_db(all_messages_to_save)
        logger.info("Cинхронизация данных с Авито завершена успешно")
    
    except Exception as e:
        logger.error(f"Ошибка функции main_avito_data: {e}")
        
async def main_llm_data():
    try:
        logger.info("Получение чатов для анализа")
        chat_ids = await database.get_chats_for_analysis()

        if not chat_ids:
            logger.info("Нет новых чатов для анализа.")

        logger.info("Чаты получены, начинаю анализ...")

        semaphore = asyncio.Semaphore(10)

        async def process_chat(chat_id):
            async with semaphore:
                try:
                    chat_data = await database.get_chat_data_for_analysis(chat_id)
                    prompt_data = utils.create_prompt(chat_data)
                    analysis_result = await llm.send_to_deepseek(prompt_data)
                    mapped_data = utils.map_response_llm(analysis_result, chat_id, chat_data)
                    await database.save_reports_to_db(mapped_data)

                except Exception as e:
                    logger.error(f"Ошибка при обработке чата {chat_id}: {e}")
    
        tasks = [process_chat(chat_id) for chat_id in chat_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка задачи: {result}")
    
        logger.info(f"Анализ {len(chat_ids)} чатов завершен")

    except Exception as e:
        logger.error(f"Ошибка функции main_llm_data: {e}")
 
async def send_reports_on_timer():
    try:    
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59)
        
        reports = await database.get_reports_from_db(start_date, end_date)
        users = await database.get_all_active_users()
        
        for user in users:
                
                await bot.send_message(
                    chat_id=user,
                    text=f"<b>Ежедневный отчет за {yesterday.strftime('%d.%m.%Y')}</b>\n\n"
                         f"Всего отчетов: {len(reports)}",
                    parse_mode='HTML'
                )   
                for report in reports:
                    report_text = utils.format_single_report(report)
                    await bot.send_message(
                        chat_id=user,
                        text=report_text,
                        parse_mode='HTML'
                    )
                    await asyncio.sleep(random.uniform(1.5, 3.5))

    except Exception as e:
        logger.error(f"Ошибка в функции send_reports_on_timer: {e}")       

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user = message.from_user
    user_data = {
        'user_id': message.from_user.id,
        'username': message.from_user.username,
        'first_name': message.from_user.first_name,
        'last_name': message.from_user.last_name
    }
    await database.add_user_to_db(user_data)

    name = user.first_name
    welcome_text = f"""
👋 <b>Добро пожаловать, {name}!</b>

🤖 Я - бот для анализа диалогов Авито

📊 <b>Что я умею:</b>
• Автоматически анализировать переписки с клиентами
• Формировать отчеты по качеству коммуникации
• Присылать ежедневные отчеты
• Присылать отчеты за выбранный период

💡 <b>Как получить отчет:</b>
• Нажмите кнопку в меню <b>"Сформировать отчет"</b>
или
• Используйте команду /report

После этого введите дату начала периода и дату конца периода

⏰ <b>Ежедневная рассылка:</b>
Отчеты будут приходить автоматически каждый день в 10:00

ℹ️  <b>Для подробной справки по боту:</b>
• Нажмите кнопку в меню <b>"Помощь"</b>
или
• Используйте команду /help

Рад быть полезным! 🚀
"""
    await message.answer(welcome_text, parse_mode='HTML')

@dp.message(Command("report"))
async def cmd_report(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите начальную дату периода\n\n"
        "Пример: 01.09.2025",
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
            reports = await database.get_reports_from_db(start_date, end_date)

            if not reports:
                await message.answer("Отчеты за указанный период отсутствуют")
                return

            for report in reports:
                report_text = utils.format_single_report(report)
                await message.answer(report_text, parse_mode='HTML')
                await asyncio.sleep(random.uniform(1.5, 3.5))

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
            "<b>🤖 Команда не распознана</b>\n\n"
            "Доступные команды:\n"
            "• /start - Запустить бота\n"  
            "• /report - Сформировать отчет за период\n"
            "• /help - Помощь\n",
            parse_mode='HTML',
        )
        
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--command')
    args = parser.parse_args()

    async def main():
        try:
            await database.create_db_pool()
            
            if args.command == 'polling':
                scheduler = setup_scheduler()
                scheduler.start()
                await bot.delete_webhook(drop_pending_updates=True)
                await dp.start_polling(bot)

            else:
                if args.command == 'avito':
                    await main_avito_data()
                elif args.command == 'llm':
                    await main_llm_data()
                elif args.command == 'timer':
                    await send_reports_on_timer() 

        finally:
            if 'scheduler' in locals():
                scheduler.shutdown() 
            await database.close_db_pool()
            if bot.session:
                await bot.session.close()

    asyncio.run(main())