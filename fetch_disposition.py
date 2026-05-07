import requests
import json
import pandas as pd
import yfinance as yf
import re
import time
import random
from datetime import datetime

# 禁用 SSL 警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_disposition_stocks():
    """ 抓取上市與上櫃處置股 """
    results = []
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    
    # 上市
    print("Fetching TWSE disposition stocks...")
    twse_url = f"https://www.twse.com.tw/rwd/zh/announcement/disposition?response=json&_={int(time.time()*1000)}"
    try:
        resp = session.get(twse_url, headers=headers, verify=False, timeout=15)
        if resp.status_code == 200 and resp.text.strip():
            data = resp.json()
            if 'data' in data:
                for item in data['data']:
                    ticker = item[1]
                    name = item[2]
                    period = item[8]
                    dates = re.findall(r'\d{3}/\d{2}/\d{2}', period)
                    end_date = dates[-1] if dates else "未知"
                    results.append({
                        "ticker": f"{ticker}.TW",
                        "name": name,
                        "end_date": end_date
                    })
        else:
            print(f"TWSE returned status {resp.status_code} or empty body")
    except Exception as e:
        print(f"Error fetching TWSE: {e}")

    # 上櫃
    print("Fetching TPEx disposition stocks...")
    tpex_url = f"https://www.tpex.org.tw/web/bulletin/disposition/disposition_result.php?l=zh-tw&_={int(time.time()*1000)}"
    try:
        resp = session.get(tpex_url, headers=headers, verify=False, timeout=15)
        if resp.status_code == 200 and resp.text.strip():
            data = resp.json()
            if 'aaData' in data:
                for item in data['aaData']:
                    ticker = item[0]
                    name = item[1]
                    period = item[2]
                    dates = re.findall(r'\d{3}/\d{2}/\d{2}', period)
                    end_date = dates[-1] if dates else "未知"
                    results.append({
                        "ticker": f"{ticker}.TWO",
                        "name": name,
                        "end_date": end_date
                    })
        else:
            print(f"TPEx returned status {resp.status_code} or empty body")
    except Exception as e:
        print(f"Error fetching TPEx: {e}")

    return results

def fetch_fundamental_data(ticker):
    """ 抓取基本面數據 """
    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period='1mo', timeout=5)
        if hist.empty:
            close_price = 0
            mom = 0
        else:
            close_price = hist['Close'].iloc[-1]
            if len(hist) >= 2:
                prev_price = hist['Close'].iloc[0]
                mom = ((close_price - prev_price) / prev_price) * 100 if prev_price > 0 else 0
            else:
                mom = 0

        info = tk.info
        
        def clean(val, default=0):
            try:
                if val is None or pd.isna(val): return default
                return round(float(val), 2)
            except:
                return default

        return {
            "close": clean(close_price),
            "pb": clean(info.get('priceToBook')),
            "yoy": clean(info.get('revenueGrowth', 0) * 100),
            "mom": clean(mom),
            "industry": info.get('industry', '未知')
        }
    except Exception as e:
        print(f"[{ticker}] Fundamental error: {e}")
        return None

def main():
    all_disposition = get_disposition_stocks()
    
    if not all_disposition:
        print("Warning: No disposition stocks found from APIs. Adding a placeholder for UI testing.")
        # 建立一個測試用的虛擬資料，確保前端能正確渲染
        all_disposition = [{
            "ticker": "2330.TW",
            "name": "台積電 (測試用)",
            "end_date": "113/05/20"
        }]

    print(f"Processing {len(all_disposition)} stocks...")
    results = []
    for s in all_disposition:
        print(f"Processing {s['ticker']}...")
        fundamental = fetch_fundamental_data(s['ticker'])
        if fundamental:
            s.update(fundamental)
        else:
            s.update({
                "close": 0, "pb": 0, "yoy": 0, "mom": 0, "industry": "未知"
            })
        results.append(s)
        time.sleep(random.uniform(0.3, 0.8))
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    output = {
        "scan_date": today_str,
        "data_date": today_str,
        "data": results
    }
    
    with open('disposition_data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    
    print(f"Saved {len(results)} stocks to disposition_data.json")

if __name__ == "__main__":
    main()
