#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import re
import urllib.request
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import os

# timeout:超出时间 等待的最长时间(同时要考虑隐形等待时间)
# 显示等待
driver = webdriver.Chrome()
wait = WebDriverWait(driver, 8)


class MusicInfo(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def get_music_info(self):
        url = "https://music.163.com/#/artist?id={0}".format(self.id)
        driver.get(url)
        driver.switch_to.frame('contentFrame')
        # with open('data/source.html','w',encoding='utf-8') as f:
        #     f.write(driver.page_source)
        # 获取歌手的姓名，并建立对应文件夹
        # artist_name = driver.find_element_by_id('artist-name').text
        print(self.name)
        path = os.getcwd() + "/data/{0}".format(self.name)
        if not os.path.exists(path):
            os.makedirs(path)
        print(path)
        tr_list = driver.find_element_by_id("hotsong-list").find_elements_by_tag_name("tr")
        music_info = []
        for i in range(len(tr_list)):
            content = tr_list[i].find_element_by_class_name('txt')
            href = content.find_element_by_tag_name('a').get_attribute('href')
            title = content.find_element_by_tag_name('b').get_attribute('title')
            music_info.append((title, href))
        return music_info, path

    def save_csv(self, music_info, path, head=None):
        data = pd.DataFrame(music_info, columns=head)
        # index=False去掉DataFrame默认的index列
        data.to_csv("{0}/singer{1}.csv".format(path, str(self.id)), encoding="utf-8", index=False)


class DownloadMusic(object):
    def __init__(self, music_name, music_id, path):
        self.music_name = music_name
        self.music_id = music_id
        self.path = path

    def get_lyric(self):
        url = 'http://music.163.com/api/song/lyric?' + 'id=' + str(self.music_id) + '&lv=1&kv=1&tv=-1'
        r = requests.get(url)
        raw_json = r.text
        ch_json = json.loads(raw_json)
        raw_lyric = ch_json['lrc']['lyric']
        del_str = re.compile(r'\[.*\]')
        ch_lyric = re.sub(del_str, '', raw_lyric)
        return ch_lyric

    def download_mp3(self):
        url = 'http://music.163.com/song/media/outer/url?id=' + str(self.music_id) + '.mp3'
        try:
            print("正在下载：{0}".format(self.music_name))
            path = self.path + "/music"
            if not os.path.exists(path):
                os.makedirs(path)
            urllib.request.urlretrieve(url, '{0}/{1}.mp3'.format(path, self.music_name))
            print("Finish...")
        except:
            print("Failed...")

    def save_txt(self):
        lyric = self.get_lyric()
        print(lyric)
        print("正在写入歌曲:{0}".format(self.music_name))
        path = self.path + "/lyric"
        if not os.path.exists(path):
            os.makedirs(path)
        with open("{0}/{1}.txt".format(path, "".join(self.music_name.replace('.', '').replace('?', '').split())), 'w',
                  encoding='utf-8') as f:
            f.write(lyric)


def main(id, name):
    mu_info = MusicInfo(id, name)  # 类初始化
    music_info, path = mu_info.get_music_info()  # 调用方法，获取音乐信息及路径
    mu_info.save_csv(music_info, path, head=['music', 'link'])  # 存储音乐的歌名及链接至csv文件
    '''
    调用pandas的read_csv()方法时，默认使用C engine作为parser engine，而当文件名中含有中文的时候,就会报错，
    这里一定要设置engine为python，即engine='python'
    '''
    mu_info = pd.read_csv('{0}/singer{1}.csv'.format(path, str(id)), engine='python', encoding='utf-8')
    '''
    通过iterrows遍历音乐信息的music文件
    iterrows返回的是一个元组(index,mu)
    '''
    for index, mu in mu_info.iterrows():
        music = mu['music']  # 取对应的歌曲名称 mu['link']音乐的链接
        regex = re.compile(r'(id)(=)(.*)')
        link = re.search(regex, mu['link']).group(3)
        print(link)
        music = DownloadMusic(music, link, path)
        music.save_txt()
        music.download_mp3()


if __name__ == '__main__':
    dict_data = {
        '5781': '薛之谦',
        '2116': '陈奕迅',
        '3684': '林俊杰',
        '44266': 'Taylor Swift',
        '72724': 'Rihanna',
    }
    for id, name in dict_data.items():
        print(id, name)
        main(id, name)
    main(12138269, '毛不易')
