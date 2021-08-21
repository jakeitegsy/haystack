from datetime import datetime

class Logger:

    def __init__(self, ticker):
        self.ticker = ticker
        
    def log(self, message=None, prefix=''):
        print(f'{prefix}{datetime.now()}::Ticker::{self.ticker}::{message}')

    def error(self, message):
        return self.log(f'{message}::FAILED::', prefix='[ERROR] ')

    def success(self, message):
        return self.log(f'{message}Succeeded::')