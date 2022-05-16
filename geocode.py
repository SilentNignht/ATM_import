import configparser
import os
import pymysql
import requests
import json
import csv

# https://geocode.search.hereapi.com/v1/geocode?q=%E5%8F%B0%E5%8C%97%E5%B8%82%E5%B8%82%E5%BA%9C%E8%B7%AF1%E8%99%9F&apikey=YOUR_API_KEY&at=25.03752,121.56442&lang=zh-TW&in=countryCode:TWN

# 讀取設定檔案
config = configparser.RawConfigParser()
cwd = os.getcwd()
config_path = os.path.join(cwd, 'config.ini')
config.read(config_path)

# Here API 參數
base_url = 'https://geocode.search.hereapi.com/v1/geocode'
api_key = config.get('atm-import', 'here_apikey')
query_at = '25.03752,121.56442'
query_lang = 'zh-TW'
query_in = 'countryCode:TWN'

csv_path = os.path.join(cwd, 'A2_Location.csv')
a = []
# 開啟原始資料，記錄到list裡
with open(csv_path, "r", newline='', encoding="utf-8") as csv_reader:
    next(csv_reader)
    rows = csv.reader(csv_reader)
    for i in rows:
        a.append(i)

# 將地址資訊轉換成經緯度後，寫入新資料中
b = []
for d in a:
    query_addr = d[4]
    query_data = f"?q={query_addr}&apikey={api_key}&at={query_at}&lang={query_lang}&in{query_in}"

    try:
        # TODO: 從資料庫撈出地址資訊, 透過以上的程式碼來撈經緯度資料，再存回資料庫
        first_response = requests.get(base_url+query_data)
        d.append(first_response.json()['items'][0]['position']['lat'])
        d.append(first_response.json()['items'][0]['position']['lng'])
        b.append(d)
        with open("latlng.csv", "a", newline='') as csv_r:
            csv_writer = csv.writer(csv_r)
            csv_writer.writerow(d)
        print(d)

    except IndexError:
        b.append(d)
        print("找不到經緯度")
