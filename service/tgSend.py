import requests
import config

token = config.tg_token


def message_to(text):
    requests.get('https://api.telegram.org/bot{}/sendMessage'.format(token), params=dict(chat_id='@anyObjectBot', text=text))