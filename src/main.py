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

class ReportState(StatesGroup):
    waiting_for_period_selection = State()
    waiting_for_start_date = State()
    waiting_for_end_date = State()
    showing_reports = State()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DIKON_ID = os.getenv("DIKON_USER_ID")

moscow_tz = timezone(timedelta(hours=3))
bot = Bot(token=TOKEN)
dp = Dispatcher()

def get_period_selection_keyboard():
    keyboard = [
        [
            types.InlineKeyboardButton(text="📅 За день", callback_data="period_day"),
            types.InlineKeyboardButton(text="📅 За неделю", callback_data="period_week"),
        ],
        [
            types.InlineKeyboardButton(text="📅 За месяц", callback_data="period_month"),
            types.InlineKeyboardButton(text="📅 Свой период", callback_data="period_custom"),
        ],
        [
            types.InlineKeyboardButton(text="❌ Отмена", callback_data="period_cancel"),
        ]
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_reports_navigation_keyboard(current_index, total_reports, has_next):
    keyboard = []
    if has_next:
        keyboard.append([
            types.InlineKeyboardButton(
                text=f"▶️ Следующий ({current_index + 1}/{total_reports})", 
                callback_data="next_report"
            )
        ])
    keyboard.append([
        types.InlineKeyboardButton(text="❌ Завершить просмотр", callback_data="cancel_reports")
    ])
    return types.InlineKeyboardMarkup(inline_keyboard=keyboard)

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

async def show_single_report(chat_id, state: FSMContext):
    data = await state.get_data()
    reports = data['reports']
    current_index = data['current_index']
    total_reports = data['total_reports']
    
    report = reports[current_index]
    report_text = utils.format_single_report(report)

    header = f"📊 Сформировано отчетов: {total_reports}\n"
    numbered_text = f"{header}{report_text}"
    has_next = current_index < total_reports - 1
    
    if current_index == 0:
        await bot.send_message(
            chat_id=chat_id,
            text=numbered_text,
            parse_mode='HTML',
            reply_markup=get_reports_navigation_keyboard(current_index, total_reports, has_next)
        )
    else:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=data.get('last_message_id'),
            text=numbered_text,
            parse_mode='HTML',
            reply_markup=get_reports_navigation_keyboard(current_index, total_reports, has_next)
        )
    
    await state.set_state(ReportState.showing_reports)

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
• Показывать отчеты по требованию

💡 <b>Как получить отчет:</b>
• Выберите в меню <b>"Сформировать отчет"</b>
или
• Используйте команду <b>/report</b>

⏰ <b>Ежедневная рассылка:</b>
Отчеты будут приходить автоматически каждый день в 10:00

ℹ️  <b>Для подробной справки по боту:</b>
• Выберите в меню <b>"Помощь"</b>
или
• Используйте команду <b>/help</b>

Рад быть полезным! 🚀
"""
    await message.answer(welcome_text, parse_mode='HTML')

@dp.message(Command("report"))
async def cmd_report(message: types.Message, state: FSMContext):
    await message.answer(
        "📊 <b>Формирование отчета за период</b>\n\n"
        "Выберите период или укажите свой:",
        parse_mode='HTML',
        reply_markup=get_period_selection_keyboard()
    )
    await state.set_state(ReportState.waiting_for_period_selection)

@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    
    if current_state is None:
        await message.answer("Нет активных операций для отмены")
        return
    
    await state.clear()
    await message.answer(
        "✅ <b>Операция отменена</b>\n\n",
        parse_mode='HTML'
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    help_text = """
🤖 <b>Справка по боту анализа диалогов Авито</b>

📊 <b>Основные команды:</b>

• <b>/start</b> - Запустить бота и ознакомиться с возможностями
• <b>/report</b> - Сформировать отчет за выбранный период
• <b>/help</b> - Показать эту справку
• <b>/cancel</b> - Отменить операцию

⏰ <b>Автоматические отчеты:</b>
Бот автоматически присылает ежедневные отчеты каждый день в <b>10:00</b>

📅 <b>Как получить отчет за период:</b>
1. Нажмите <b>/report</b> или выберите в меню
2. Выберите период из предложенных или укажите свой
3. Если выбран "Свой период" - введите даты в формате <b>ДД.ММ.ГГГГ</b>
4. Получите отчеты с навигацией по страницам

🔍 <b>Что анализируется в отчетах:</b>
• Тональность коммуникации
• Профессионализм менеджера  
• Ясность изложения информации
• Решение проблем клиента
• Работа с возражениями
• Завершение диалога

📱 <b>Навигация по отчетам:</b>
• Используйте кнопку <b>"▶️ Следующий"</b> для перехода к следующему отчету
• Кнопка <b>"❌ Завершить просмотр"</b> завершает текущую сессию

⚙️ <b>Техническая информация:</b>
• Данные синхронизируются с Авито автоматически
• Анализ проводится с помощью AI-модели DeepSeek
• Все отчеты сохраняются в базе данных

💡 <b>Совет:</b> Для быстрого доступа к отчетам используйте команду <b>/report</b>
"""
    await message.answer(help_text, parse_mode='HTML')

@dp.callback_query(ReportState.waiting_for_period_selection)
async def process_period_selection(callback: types.CallbackQuery, state: FSMContext):
    now = datetime.now()
    
    if callback.data == "period_cancel":
        await callback.message.edit_text("✅ <b>Выбор периода отменен</b>", parse_mode='HTML')
        await state.clear()
        await callback.answer()
        return
    
    elif callback.data == "period_day":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        period_text = "сегодня"
        
    elif callback.data == "period_week":
        start_date = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        period_text = "неделю"
        
    elif callback.data == "period_month":
        start_date = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        period_text = "месяц"
        
    elif callback.data == "period_custom":
        await callback.message.edit_text(
            "📊 <b>Формирование отчета за период</b>\n\n"
            "👟 <b>Шаг 1 из 2:</b> Введите начальную дату\n\n"
            "📅 <b>Формат:</b> ДД.ММ.ГГГГ\n\n"
            "✨ <b>Пример:</b> 01.09.2025\n\n"
            "💡 <b>Для отмены используйте команду</b> /cancel",
            parse_mode='HTML'
        )
        await state.set_state(ReportState.waiting_for_start_date)
        await callback.answer()
        return
    
    await callback.message.edit_text(f"🔍 <b>Отчеты за {period_text}...</b>", parse_mode='HTML')
    
    reports = await database.get_reports_from_db(start_date, end_date)

    if not reports:
        await callback.message.edit_text(f"❌ <b>Отчеты за {period_text} отсутствуют</b>", parse_mode='HTML')
        await state.clear()
        await callback.answer()
        return
    
    await state.update_data(
        reports=reports,
        current_index=0,
        total_reports=len(reports)
    )

    await show_single_report(callback.message.chat.id, state)
    await callback.answer()

@dp.message(ReportState.waiting_for_period_selection)
async def control_period_selection(message: types.Message, state: FSMContext):
    await message.answer(
        "<b>Пожалуйста, выберите период с помощью кнопок ниже</b>",
        parse_mode='HTML',
        reply_markup=get_period_selection_keyboard()
    )

@dp.message(ReportState.waiting_for_start_date)
async def process_start_date(message: types.Message, state: FSMContext):
    if message.text == '/cancel':
        await cmd_cancel(message, state)
        return
    if not message.text or not isinstance(message.text, str):
        await message.answer(
            "❌ <b>Не вижу дату</b>\n\n"
            "<b>Пожалуйста, введите начальную дату в формате:</b>\n"
            "ДД.ММ.ГГГГ\n\n"
            "<b>Пример:</b>\n"
            "01.09.2025\n\n"
            "💡 <b>Для отмены используйте команду</b> /cancel",
            parse_mode='HTML'
        )
        return
    try:
        start_date = datetime.strptime(message.text, '%d.%m.%Y')
        await state.update_data(start_date=start_date)
        await message.answer(
            "📊 <b>Формирование отчета за период</b>\n\n"
            "👟 <b>Шаг 2 из 2:</b> Введите конечную дату\n\n"
            "📅 <b>Формат:</b> ДД.ММ.ГГГГ\n\n"
            "✨ <b>Пример:</b> 01.09.2025\n\n"
            "💡 <b>Для отмены используйте команду</b> /cancel",
            parse_mode='HTML'
        )
        await state.set_state(ReportState.waiting_for_end_date)
    except ValueError:
        await message.answer(
            "❌ <b>Не вижу дату</b>\n\n"
            "<b>Пожалуйста, введите начальную дату в формате:</b>\n"
            "ДД.ММ.ГГГГ\n\n"
            "<b>Пример:</b>\n"
            "01.09.2025\n\n"
            "💡 <b>Для отмены используйте команду</b> /cancel",
            parse_mode='HTML'
        )
                
@dp.message(ReportState.waiting_for_end_date)
async def process_end_date(message: types.Message, state: FSMContext):
    if message.text == '/cancel':
        await cmd_cancel(message, state)
        return
    if not message.text or not isinstance(message.text, str):
        await message.answer(
            "❌ <b>Не вижу дату</b>\n\n"
            "<b>Пожалуйста, введите конечную дату в формате:</b>\n"
            "ДД.ММ.ГГГГ\n\n"
            "<b>Пример:</b>\n"
            "01.09.2025\n\n"
            "💡 <b>Для отмены используйте команду</b> /cancel",
            parse_mode='HTML'
        )
        return 
      
    try:
        end_date_input = datetime.strptime(message.text, '%d.%m.%Y')
        end_date = end_date_input.replace(hour=23, minute=59, second=59)
        data = await state.get_data()
        start_date = data['start_date']
        
        reports = await database.get_reports_from_db(start_date, end_date)

        if not reports:
            await message.answer("❌ <b>Отчеты за указанный период отсутствуют</b>", parse_mode='HTML')
            await state.clear()
            return
        
        await state.update_data(
            reports=reports,
            current_index=0,
            total_reports=len(reports)
        )

        await show_single_report(message.chat.id, state)

    except ValueError:
        await message.answer(
            "❌ <b>Не вижу дату</b>\n\n"
            "<b>Пожалуйста, введите конечную дату в формате:</b>\n"
            "ДД.ММ.ГГГГ\n\n"
            "<b>Пример:</b>\n"
            "01.09.2025\n\n"
            "💡 <b>Для отмены используйте команду</b> /cancel",
            parse_mode='HTML'
        )     

@dp.callback_query(lambda c: c.data == "next_report", ReportState.showing_reports)
async def next_report_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_index = data['current_index']
    
    await state.update_data(
        current_index=current_index + 1,
        last_message_id=callback.message.message_id
    )
    
    await show_single_report(callback.message.chat.id, state)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_reports", ReportState.showing_reports)
async def cancel_reports_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    total_reports = data.get('total_reports', 0)
    viewed_reports = data.get('current_index', 0) + 1
    
    await callback.message.edit_text(
        f"✅ <b>Просмотр завершен</b>\n\n"
        f"Просмотрено отчетов: {viewed_reports} из {total_reports}",
        parse_mode='HTML'
    )
    await state.clear()
    await callback.answer()

@dp.message()
async def block_all_messages(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    
    if current_state is None:
        await message.answer(
            "<b>🤖 Команда не распознана</b>\n\n"
            "Доступные команды:\n"
            "• /start - Запустить бота\n"  
            "• /report - Сформировать отчет за период\n"
            "• /cancel - Отменить операцию\n"
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