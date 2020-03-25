#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import datetime
import time
import random
from InfoQ.tool.header import headers
import requests
from InfoQ.logger.log import crawler, storage
from InfoQ.db.mongo_helper import Mongo


class InfoQ(object):
    def __init__(self):
        self.start_url = "https://www.infoq.cn/public/v1/my/recommond"
        self.headers = headers
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_req(self, data=None):
        req = self.session.post(self.start_url, data=json.dumps(data))
        if req.status_code in [200, 201]:
            return req

    def save(self, data):
        tasks = []
        for item in data:
            try:
                dic = {}
                uuid = item.get("uuid")
                dic["uuid"] = uuid  # 经过分析发现uuid就是详情页链接的组成部分。
                dic["url"] = f"https://www.infoq.cn/article/{uuid}"
                dic["title"] = item.get("article_title")
                dic["cover"] = item.get("article_cover")
                dic["summary"] = item.get("article_summary")
                author = item.get("author")
                if author:
                    dic["author"] = author[0].get("nickname")
                else:
                    dic["author"] = item.get("no_author", "").split(":")[-1]
                score = item.get("publish_time")
                dic["publish_time"] = datetime.datetime.utcfromtimestamp(score / 1000).strftime("%Y-%m-%d %H:%M:%S")
                dic["tags"] = ",".join([data.get("name") for data in item.get("topic")])
                translate = item.get("translator")
                dic["translator"] = dic["author"]
                if translate:
                    dic["translator"] = translate[0].get("nickname")
                dic["status"] = 0
                dic["update_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                tasks.append(dic)
            except IndexError as e:
                crawler.error("解析出错", e)
        Mongo().save_mongo_data(tasks)
        crawler.info(f"add {len(tasks)} datas to mongodb")
        return score

    def start(self):
        i = 0
        post_data = {"size": 12}
        while i < 5:  # 这里只爬取了5页 这个值可以自己设置
            req = self.get_req(post_data)
            data = req.json().get("data")
            score = self.save(data)
            post_data.update({"score": score})  # 通过上一页的内容得知下一次要请求的参数。
            i += 1
            time.sleep(random.randint(0, 3))


if __name__ == '__main__':
    info = InfoQ()
    info.start()
