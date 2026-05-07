import requests
import pandas as pd
import io
import time

def test_csv_twse():
    print("Testing TWSE CSV...")
    # TWSE CSV endpoint
    url = "https://www.twse.com.tw/rwd/zh/announcement/disposition?response=csv"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            # TWSE CSV usually starts with some titles, we might need to skip rows
            print(f"Sample: {resp.text[:200]}")
    except Exception as e:
        print(f"Error: {e}")

def test_csv_tpex():
    print("\nTesting TPEx CSV...")
    # TPEx CSV endpoint
    url = "https://www.tpex.org.tw/web/bulletin/disposition/disposition_result.php?l=zh-tw&o=csv"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"Sample: {resp.text[:200]}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_csv_twse()
    test_csv_tpex()
