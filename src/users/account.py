from src.response import AsyncHttpClient


class Account:
    def __init__(self, account, token):
        self.account = account
        self.token = token
        self.async_client = AsyncHttpClient(throttle_scope=account)
        self.headers = {"Authorization": token, 'Content-Type': 'application/json'}


