from src.response import AsyncHttpClient, HttpClient


class Account:
    def __init__(self, account, token):
        self.account = account
        self.token = token
        self.async_client = AsyncHttpClient()
        self.sync_client = HttpClient()
        self.headers = {"Authorization": token, 'Content-Type': 'application/json'}


