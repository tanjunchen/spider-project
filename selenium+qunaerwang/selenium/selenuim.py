#!/usr/bin/python
# -*- coding: UTF-8 -*-

from selenium import webdriver

driver = webdriver.Chrome()
driver.get("D:\PythonProject\Spider\phantomjs\selenium\login.html")
username = driver.find_element_by_name('username')
password = driver.find_element_by_xpath(".//*[@id='loginForm']/input[2]")
login_button = driver.find_element_by_xpath("//input[@type='submit']")
username.send_keys('hah')
password.send_keys('hhhhh')
login_button.click()

username.clear()
password.clear()
