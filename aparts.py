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
QUERY_PARAMS = os.environ.get('QUERY_PARAMS', '')
POLL_INTERVAL = int(os.environ['POLL_INTERVAL']) # seconds
REDIS_HOST = os.environ['REDIS_HOST']
HIGHLIGHT_RULES_CONFIG = os.environ.get('HIGHLIGHT_RULES', '')


QUERY_URL = f'https://www.olx.ua/d/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/{CITY}/?search[order]=created_at:desc'
if QUERY_PARAMS != '':
    QUERY_URL += f'&{QUERY_PARAMS}'

SEND_MESSAGE_URL_FORMAT = f'https://api.telegram.org/bot{BOT_API_KEY}/sendMessage?chat_id={CHAT_ID}&text={{text}}'
MAX_REFRESH_TIME_REDIS_KEY = f'MRT_{CITY}_{CHAT_ID}'


REDIS_CONNECTION = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
HIGHLIGHT_RULES = []
if HIGHLIGHT_RULES_CONFIG != '':
    HIGHLIGHT_RULES = highlights.parse_rules(json.loads(HIGHLIGHT_RULES_CONFIG))


def log(message: str):
    print(f'{datetime.utcnow().isoformat()}: {message}')


@dataclass
class Ad:
    url: str
    created: datetime
    refreshed: datetime
    is_promoted: bool
    highlights: str

    def __str__(self) -> str:
        return f'''
{self.url}
Created: {self.created}
Refreshed: {self.refreshed}
{self.highlights}
'''


def get_ads():
    page = 1
    while True:
        url = QUERY_URL
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
                url=olxAd['url'],
                created=datetime.fromisoformat(olxAd['createdTime']),
                refreshed=datetime.fromisoformat(olxAd['lastRefreshTime']),
                is_promoted=olxAd['isPromoted'],
                highlights=highlights,
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
        encoded_text = urllib.parse.quote(str(ad))
        url = SEND_MESSAGE_URL_FORMAT.format(text=encoded_text)

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
    log(f'Application is starting\nQUERY_URL={QUERY_URL}\nPOLL_INTERVAL={POLL_INTERVAL}')

    while True:
        post_new_ads()
        time.sleep(POLL_INTERVAL)
