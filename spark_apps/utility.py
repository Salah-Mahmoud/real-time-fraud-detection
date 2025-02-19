import requests

BOT_TOKEN = "804****363:AAFSByA*********eszrT5xnXBtOJ6FJhWxQ"
CHAT_ID = "115*****6402"

def send_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    requests.post(url, data=payload)

