#coding:utf-8
from selenium import webdriver
import os
import chardet
import time
import sys
import codecs
import threading
import redis
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
dic = {"shang": "0", "shen": "1", "chuang":"1"}

def redis_connect():
    re = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
    return re


def spider(code, code_type):
    re = redis_connect()

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': 'C:\python_project\gupiao\csv\\'}
    chrome_options.add_experimental_option('prefs', prefs)

    path = "tools/chromedriver.exe"
    driver = webdriver.Ie(executable_path=path, options=chrome_options)
    driver.get("http://quotes.money.163.com/trade/lsjysj_{}.html?year={}&season={}".format(code, 2020, 1))
    start_date = driver.find_element_by_xpath("//*[@name='date_start_value']").get_attribute('value')
    end_date = driver.find_element_by_xpath("//*[@name='date_end_value']").get_attribute('value')
    download_url = "http://quotes.money.163.com/service/chddata.html?code=" + code_type + code + "&start=" + start_date + "&end=" + end_date + "&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP"
    driver.get(download_url)
    path = "csv/"+code+".csv"
    while not os.path.exists(path):
        time.sleep(1)
    print("download {} success".format(code))
    driver.close()


def spider_thread(code_lst, code_type):
    for code in code_lst:
        spider(code, dic[code_type])


def spider_threads(code_type):
    codes_list = []
    for i in range(40):
        codes_list.append([])
    cnt = 0
    thread_num = 2
    for line in open("resource/{}.txt".format(code_type), encoding="utf-8"):
        if len(line) == 0:
            continue
        code, name = line.split("\t")
        codes_list[cnt].append(code)
        cnt = (cnt + 1) % thread_num

    for i in range(thread_num):
        t = threading.Thread(target=spider_thread,args=(codes_list[i], code_type))
        t.start()


spider_threads("shang")
spider_threads("shen")
spider_threads("chuang")
re = redis_connect()
print("finished")
#os.system("mv /c/Users/wpj/Downloads/*.PDF ./yjx/")
