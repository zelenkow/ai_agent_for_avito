import os
import aiohttp
import json
from dotenv import load_dotenv

load_dotenv()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

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