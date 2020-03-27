# !/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pandas as pd
from selenium import webdriver


class GovCn(object):
    def __init__(self):
        self.driver = webdriver.Chrome()
        self.all_data = pd.DataFrame()
        self.url = "https://www.tzxm.gov.cn:8081/tzxmspweb/tzxmweb/pages/portal/publicinformation/examine_new.jsp?apply_date_begin=2016-01-01&project_area=03"

    def get_content(self):
        div_list = self.driver.get(self.url)
        self.driver.implicitly_wait(30)


if __name__ == '__main__':
    job()
