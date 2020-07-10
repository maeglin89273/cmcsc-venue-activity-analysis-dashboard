import json
from datetime import datetime

from tornado.httpclient import AsyncHTTPClient
from preprocess import raw_json_preprocess
SOURCE_API = 'https://cmcsc.cyc.org.tw/api'


class DataFetcher:
    def __init__(self, raw_data_transform=raw_json_preprocess):
        self.http_client = AsyncHTTPClient()
        self.raw_data_transform = raw_data_transform




    async def fetch_data(self):
        response = await self.http_client.fetch(SOURCE_API)
        data = json.loads(response.body.decode())
        return self.raw_data_transform(data)

