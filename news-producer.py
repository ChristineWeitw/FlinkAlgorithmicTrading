import json
from typing import List
from datetime import datetime
from alpaca_trade_api import REST
from alpaca_trade_api.common import URL
from alpaca.common import Sort
from kafka import KafkaProducer

from alpaca_config.keys import config
# from utils import get_sentiment

def get_producer(brokers: List[str]):
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer


def produce_history_news(
        redpandas_client: KafkaProducer,
        start_date: str,
        end_date: str,
        symbols: List[str],
        topic: str,
):
    key_id = config['APCA_API_KEY_ID']
    secret_key = config['APCA_API_SECRET_KEY']
    base_url = config['base_url']

    api = REST(key_id=key_id, 
               secret_key=secret_key, 
               base_url=URL(base_url))

    for symbol in symbols:
        news = api.get_news(
            symbol=symbol,
            start=start_date,
            end=end_date,
            limit=5,
            sort=Sort.ASC,
            include_content=False,
        )
        print(news)

if __name__ == '__main__':
    produce_history_news(
        get_producer(config['redpanda_brokers']),
        topic='market-news',
        start_date='2024-01-01',
        end_date='2024-06-17',
        symbols=['NVDA','Nvidia']
    )