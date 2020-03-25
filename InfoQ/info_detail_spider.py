#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import aiohttp
import aiofiles
import async_timeout
import asyncio
from InfoQ.logger.log import crawler, storage
from InfoQ.db.motor_helper import MotorBase
import datetime
import json
from w3lib.html import remove_tags
from aiostream import stream
from async_retrying import retry

base_url = "https://www.infoq.cn/public/v1/article/getDetail"
headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Type": "application/json",
    "Host": "www.infoq.cn",
    "Origin": "https://www.infoq.cn",
    "Referer": "https://www.infoq.cn/article/Ns2yelhHTd0rhmu2-IzN",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
}

headers2 = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
}
try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

sema = asyncio.Semaphore(5)


async def get_buff(item, session):
    url = item.get("cover")
    with async_timeout.timeout(60):
        async with session.get(url) as r:
            if r.status == 200:
                buff = await r.read()
                if len(buff):
                    crawler.info(f"NOW_IMAGE_URL:, {url}")
                    await get_img(item, buff)


async def get_img(item, buff):
    # 题目层目录是否存在
    file_path = item.get("file_path")
    image_path = item.get("image_path")
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # 文件是否存在
    if not os.path.exists(image_path):
        storage.info(f"SAVE_PATH:{image_path}")
        async with aiofiles.open(image_path, 'wb') as f:
            await f.write(buff)


async def get_content(source, item):
    dic = {}
    dic["uuid"] = item.get("uuid")
    dic["title"] = item.get("title")
    dic["author"] = item.get("author")
    dic["publish_time"] = item.get("publish_time")
    dic["cover_url"] = item.get("cover")
    dic["tags"] = item.get("tags")
    dic["image_path"] = item.get("image_path")
    dic["md5name"] = item.get("md5name")
    html_content = source.get("data").get("content")
    dic["html"] = html_content
    dic["content"] = remove_tags(html_content)
    dic["update_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await MotorBase().save_data(dic)


@retry(attempts=5)
async def fetch(item, session, retry_index=0):
    '''
    对内容的下载 重试次数5次
    :param item:
    :param session:
    :param retry_index:
    :return:
    '''
    refer = item.get("url")
    uuid = item.get("uuid")
    if retry_index == 0:
        await MotorBase().change_status(uuid, 1)  # 开始下载
    data = {"uuid": uuid}
    headers["Referer"] = refer
    with async_timeout.timeout(60):
        async with session.post(url=base_url, headers=headers, data=json.dumps(data)) as req:
            res_status = req.status
            if res_status == 200:
                json_data = await req.json()
                await get_content(json_data, item)
    await MotorBase().change_status(uuid, 2)  # 下载成功


async def bound_fetch(item, session):
    '''
    分别处理图片和内容的下载
    :param item:
    :param session:
    :return:
    '''
    md5name = item.get("md5name")
    file_path = os.path.join(os.getcwd(), "infoq_cover")
    image_path = os.path.join(file_path, f"{md5name}.jpg")
    item["md5name"] = md5name
    item["image_path"] = image_path
    item["file_path"] = file_path
    async with sema:
        await fetch(item, session)
        await get_buff(item, session)


async def branch(coros, limit=10):
    '''
    使用aiostream模块对异步生成器做一个切片操作。这里并发量为10.
    :param coros: 异步生成器
    :param limit: 并发次数
    :return:
    '''
    index = 0
    while True:
        xs = stream.preserve(coros)
        ys = xs[index:index + limit]
        t = await stream.list(ys)
        if not t:
            break
        await asyncio.ensure_future(asyncio.wait(t))
        index += limit + 1


async def run():
    '''
    入口函数
    :return:
    '''
    data = await MotorBase().find()
    crawler.info("Start Spider")
    async with aiohttp.connector.TCPConnector(limit=300, force_close=True, enable_cleanup_closed=True) as tc:
        async with aiohttp.ClientSession(connector=tc) as session:
            coros = (asyncio.ensure_future(bound_fetch(item, session)) async for item in data)
            await branch(coros)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()
