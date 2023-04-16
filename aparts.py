import os
import os.path
import requests
import re
import json
import time
import urllib.parse
import redis
import pytz
import highlights

from datetime import datetime
from dataclasses import dataclass


BOT_API_KEY = os.environ['BOT_API_KEY']
CHAT_ID = os.environ['CHAT_ID']
CITY = os.environ['CITY']
OLX_QUERY_PARAMS = os.environ.get('OLX_QUERY_PARAMS', '')
POLL_INTERVAL = int(os.environ['POLL_INTERVAL']) # seconds
REDIS_HOST = os.environ['REDIS_HOST']
HIGHLIGHT_RULES_CONFIG = os.environ.get('HIGHLIGHT_RULES', '')


OLX_URL = f'https://www.olx.ua/d/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/{CITY}/?search[order]=created_at:desc'
if OLX_QUERY_PARAMS != '':
    OLX_URL += f'&{OLX_QUERY_PARAMS}'

SEND_URL = f'https://api.telegram.org/bot{BOT_API_KEY}'
MAX_REFRESH_TIME_REDIS_KEY = f'MRT_{CITY}_{CHAT_ID}'


REDIS_CONNECTION = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
HIGHLIGHT_RULES = []
if HIGHLIGHT_RULES_CONFIG != '':
    HIGHLIGHT_RULES = highlights.parse_rules(json.loads(HIGHLIGHT_RULES_CONFIG))


def log(message: str):
    print(f'{datetime.utcnow().isoformat()}: {message}')


@dataclass
class Ad:
    title: str
    url: str
    price: str
    created: datetime
    refreshed: datetime
    is_promoted: bool
    photo_urls: list
    highlights: str

    def to_send_query(self, chat_id: str) -> str:
        text = self._get_text()
        if len(self.photo_urls) == 0:
            return f'/sendMessage?chat_id={chat_id}&text={urllib.parse.quote(text)}&parse_mode=MarkdownV2'
        elif len(self.photo_urls) == 1:
            encoded_photo_url = urllib.parse.quote(self.photo_urls[0])
            return f'/sendPhoto?chat_id={chat_id}&photo={encoded_photo_url}&caption={urllib.parse.quote(text)}&parse_mode=MarkdownV2'
        else:
            media_json = self._get_media_json(text)
            encoded_media_json = urllib.parse.quote(media_json)
            return f'/sendMediaGroup?chat_id={chat_id}&media={encoded_media_json}'

    def _get_media_json(self, caption: str) -> str:
        media = []
        for photo_url in self.photo_urls if len(self.photo_urls) <= 10 else self.photo_urls[:10]:
            media.append({
                    'type': 'photo',
                    'media': photo_url,
                })
        media[0]['caption'] = caption
        media[0]['parse_mode'] = 'MarkdownV2'
        return json.dumps(media)

    def _get_text(self) -> str:
        return f'''
[{Ad._markdown_escape(self.title)}]({self.url})
💰 *{Ad._markdown_escape(self.price)}*
➕ `{Ad._markdown_escape(self.created.isoformat())}`
🔄 `{Ad._markdown_escape(self.refreshed.isoformat())}`

{self.highlights}'''

    def _markdown_escape(text: str) -> str:
        return text.translate(str.maketrans({
            "\\": r"\\",
            "_":  r"\_",
            "*":  r"\*",
            "[":  r"\[",
            "]":  r"\]",
            "(":  r"\(",
            ")":  r"\)",
            "~":  r"\~",
            "`":  r"\`",
            ">":  r"\>",
            "#":  r"\#",
            "+":  r"\+",
            "-":  r"\-",
            "=":  r"\=",
            "|":  r"\|",
            "{":  r"\{",
            "}":  r"\}",
            ".":  r"\.",
            "!":  r"\!"
        }))


def get_ads():
    page = 1
    while True:
        url = OLX_URL
        if page > 1:
            url += f'&page={page}'

        response = requests.get(url)
        response_content = response.content.decode('utf-8')

        state_json_string = re.search(
            'window\.__PRERENDERED_STATE__\s*=\s*(".+");', response_content).group(1)
        # deserialize escaped json string
        state = json.loads(json.loads(
            f'{{"state":{state_json_string}}}')['state'])

        olxAds = state['listing']['listing']['ads']

        for olxAd in olxAds:
            highlights = ''
            for rule in HIGHLIGHT_RULES:
                highlights += rule.extract(olxAd)

            yield Ad(
                title=olxAd['title'],
                url=olxAd['url'],
                price=olxAd['price']['displayValue'],
                created=datetime.fromisoformat(olxAd['createdTime']),
                refreshed=datetime.fromisoformat(olxAd['lastRefreshTime']),
                is_promoted=olxAd['isPromoted'],
                photo_urls=olxAd['photos'],
                highlights=''.join(set(highlights)),
            )

        if page >= state['listing']['listing']['totalPages']:
            return

        page += 1


def get_max_refresh_time() -> datetime:
    redis_value = REDIS_CONNECTION.get(MAX_REFRESH_TIME_REDIS_KEY)
    if redis_value is None:
        return datetime.min.replace(tzinfo=pytz.UTC)

    return datetime.fromisoformat(redis_value)


def update_max_refresh_time(value: datetime):
    str_value = value.isoformat()
    REDIS_CONNECTION.set(MAX_REFRESH_TIME_REDIS_KEY, str_value)


def post_new_ads():
    max_refresh_time = get_max_refresh_time()

    log(f'Starting search for ads newer than {max_refresh_time.isoformat()}')
    fetch_start_time = time.time()

    new_ads = []
    for ad in get_ads():
        if not ad.is_promoted and ad.refreshed <= max_refresh_time:
            break

        if ad.refreshed > max_refresh_time:
            new_ads.append(ad)

    log(f'Got {len(new_ads)} new ad(s) from OLX in {time.time()-fetch_start_time:.3f} seconds')

    log(f'Starting sending new ads to chat')
    send_start_time = time.time()

    for ad in reversed(new_ads):
        url = SEND_URL + ad.to_send_query(CHAT_ID)
        response = requests.get(url)

        if response.status_code == 429:
            time.sleep(60)
            response = requests.get(url)

        response.raise_for_status()

        if ad.refreshed > max_refresh_time:
            max_refresh_time = ad.refreshed
            update_max_refresh_time(ad.refreshed)

    log(f'Sent {len(new_ads)} new ad(s) in {time.time()-send_start_time:.3f} seconds')


if __name__ == '__main__':
    log(f'Application is starting\nOLX_URL={OLX_URL}\nPOLL_INTERVAL={POLL_INTERVAL}')

    while True:
        post_new_ads()
        time.sleep(POLL_INTERVAL)
