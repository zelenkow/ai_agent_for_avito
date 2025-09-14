from datetime import datetime

def map_avito_chats(raw_chats_data, DIKON_ID):
    mapped_chats = []
    
    for chat in raw_chats_data.get('chats', []):
        client_name = ''
        for user in chat.get('users', []):
            if user.get('id') != DIKON_ID and user.get('name'):
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
