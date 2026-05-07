import requests
import json
import pandas as pd
from datetime import datetime

def test_twse():
    print("Testing TWSE...")
    url = "https://www.twse.com.tw/rwd/zh/announcement/disposition?response=json"
    resp = requests.get(url)
    data = resp.json()
    if 'data' in data:
        # data['data'] is a list of lists
        # Columns: 0: 公告日期, 1: 股票代號, 2: 名稱, 3: 處置措施... 8: 處置期間
        for item in data['data'][:5]:
            print(f"Code: {item[1]}, Name: {item[2]}, Period: {item[8]}")

def test_tpex():
    print("\nTesting TPEx...")
    # TPEx often uses a different format
    url = "https://www.tpex.org.tw/web/bulletin/disposition/disposition_result.php?l=zh-tw"
    resp = requests.get(url)
    data = resp.json()
    if 'aaData' in data:
        # Columns: 0: 證券代號, 1: 證券名稱, 2: 處置期間
        for item in data['aaData'][:5]:
            print(f"Code: {item[0]}, Name: {item[1]}, Period: {item[2]}")

if __name__ == "__main__":
    test_twse()
    test_tpex()
