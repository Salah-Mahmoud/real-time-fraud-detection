import requests

BOT_TOKEN = "8048788363:AAFSByAWeGGwmYKeszrT5xnXBtOJ6FJhWxQ"
CHAT_ID = "1152896402"

def send_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    requests.post(url, data=payload)

