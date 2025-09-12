from fastapi import FastAPI, Request, HTTPException, Header
import logging
import database
import uvicorn
import os
from contextlib import asynccontextmanager
from aiogram.types import Update
from main import bot, dp, setup_scheduler, main_avito_data


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler
    try:
        await database.create_db_pool()
        scheduler = setup_scheduler()
        scheduler.start()
        await bot.set_webhook("https://ai-agent-zelenkow.amvera.io/webhook")
        yield
    finally:
        scheduler.shutdown() 
        await database.close_db_pool()
        await bot.session.close()
        logger.info("Сессия бота закрыта")
            
app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()
    telegram_update = Update.model_validate(update, context={"bot": bot})
    await dp.feed_update(bot=bot, update=telegram_update)
    return {"status": "ok"}

@app.post("/sync/avito")
async def trigger_avito_sync(ApiKey: str = Header(...)):
    valid_api_key = os.getenv("APIKEY")
    if ApiKey != valid_api_key:
        raise HTTPException(
            status_code=401,
            detail="Неверный API ключ"
        )
    await main_avito_data()
        
if __name__ == "__main__":

    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)

