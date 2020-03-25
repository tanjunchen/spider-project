import requests
from ChinaHousePrice.config import headers
from datetime import datetime
import os


class SessionWrapper(object):
    def __init__(self, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = 10
        if "headers" not in kwargs:
            kwargs["headers"] = headers()
        self.args = kwargs
        if "data" not in kwargs:
            self.__session = requests.session()
        if "data" in kwargs:
            self.__session = requests.get("")

    def get(self, url, **kwargs):
        for k in self.args:
            if k not in kwargs:
                kwargs[k] = self.args[k]
        try:
            response = self.__session.get(url, **kwargs)
            return response
        except requests.exceptions.ConnectTimeout:
            num = 3
            while num > 0:
                response = self.__session.get(url, **kwargs)
                print(" 网络请求超时 正在重新连接...... ")
                num = num - 1
                if response.ok:
                    return response
            if num == 0:
                SessionWrapper.to_exception(url)
            return None

    @staticmethod
    def to_exception(url):
        today = datetime.now().strftime('%Y-%m-%d')
        flag = not os.path.exists("des_data/all_cre_result_" + today + ".csv")
        with open("exception/exception" + today + ".txt", mode="w" if flag else "a+", encoding='utf-8') as f:
            f.write(url + "\n")
