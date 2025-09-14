from fastapi import FastAPI, Request, HTTPException, Header, Depends
import logging
import database
import uvicorn
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from aiogram.types import Update
from main import bot, dp, setup_scheduler, main_avito_data, main_llm_data, send_reports_on_timer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
VALID_API_KEY = os.getenv("APIKEY")

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
        await database.close_db_pool()
        scheduler.shutdown() 
        await bot.delete_webhook()
        await bot.session.close()
            
app = FastAPI(lifespan=lifespan)

async def verify_api_key(apikey: str = Header(...)):
    if apikey != VALID_API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Неверный API ключ"
        )
    return True

@app.post("/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()
    telegram_update = Update.model_validate(update, context={"bot": bot})
    await dp.feed_update(bot=bot, update=telegram_update)
    return {"status": "ok"}

@app.post("/sync/avito")
async def trigger_avito_sync(verified: bool = Depends(verify_api_key)):
    await main_avito_data()

@app.post("/llm/analyze")
async def trigger_llm_analyze(verified: bool = Depends(verify_api_key)):
    await main_llm_data()

@app.post("/timer/reports")
async def trigger_timer_reports(verified: bool = Depends(verify_api_key)):
    await send_reports_on_timer()      
        
if __name__ == "__main__":

    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)

