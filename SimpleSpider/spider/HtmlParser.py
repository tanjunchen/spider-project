#!/usr/bin/python
# -*- coding: UTF-8 -*-


import re
import urllib.parse
from bs4 import BeautifulSoup
from lxml import etree


class HtmlParser(object):

    def parser(self, page_url, html_content):
        '''
        用于解析网页内容抽取URL和数据
        :param page_url: 下载页面的URL
        :param html_content: 下载的网页内容
        :return:返回URL和数据
        '''
        if page_url is None or html_content is None:
            return
        html = etree.HTML(html_content, 'lxml')
        new_urls = self._get_new_urls(page_url, html)
        new_data = self._get_new_data(page_url, html)
        return new_urls, new_data

    def _get_new_urls(self, page_url, html):
        '''
        抽取新的URL集合
        :param page_url: 下载页面的URL
        :param html:html
        :return: 返回新的URL集合
        '''
        new_urls = set()
        # 抽取符合要求的a标签
        links = html.xpath('.//a')
        for link in links:
            # 提取href属性
            new_url = link['href']
            # 拼接成完整网址
            new_full_url = urllib.parse.urljoin(page_url, new_url)
            new_urls.add(new_full_url)
        return new_urls

    def _get_new_data(self, page_url, html):
        '''
        抽取有效数据
        :param page_url:下载页面的URL
        :param html:html
        :return:返回有效数据
        '''
        data = {}
        data['url'] = page_url
        # summary = soup.find('div',class_='lemma-summary')
        title = html.xpath(".//dd[@class='lemmaWgt-lemmaTitle-title']/h1/text()")
        data['title'] = title
        summary = html.xpath(".//div[@class='lemma-summary']//a/text()")

        # 获取到tag中包含的所有文版内容包括子孙tag中的内容,并将结果作为Unicode字符串返回
        data['summary'] = summary
        return data
