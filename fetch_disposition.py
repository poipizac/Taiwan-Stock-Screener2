import requests
import json
import pandas as pd
import yfinance as yf
import re
import time
import random
import io
import os
from datetime import datetime

# 禁用 SSL 警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

def fetch_twse_disposition():
    """ 抓取上市處置股 (優先 JSON, 失敗則 CSV) """
    print("Fetching TWSE (Listed) disposition stocks...")
    url_json = f"https://www.twse.com.tw/rwd/zh/announcement/disposition?response=json&_={int(time.time()*1000)}"
    url_csv = "https://www.twse.com.tw/rwd/zh/announcement/disposition?response=csv"
    
    results = []
    errors = []

    # 1. 嘗試 JSON
    try:
        resp = requests.get(url_json, headers=COMMON_HEADERS, verify=False, timeout=15)
        if resp.status_code == 200 and resp.text.strip():
            if resp.text.strip().startswith('<'): # 可能是 HTML/Cloudflare
                errors.append("TWSE JSON returned HTML (Blocked)")
            else:
                data = resp.json()
                if 'data' in data:
                    for item in data['data']:
                        ticker = item[1]
                        name = item[2]
                        period = item[8]
                        dates = re.findall(r'\d{3}/\d{2}/\d{2}', period)
                        end_date = dates[-1] if dates else "未知"
                        results.append({"ticker": f"{ticker}.TW", "name": name, "end_date": end_date})
                    if results: return results, ""
        else:
            errors.append(f"TWSE JSON HTTP {resp.status_code}")
    except Exception as e:
        errors.append(f"TWSE JSON Error: {str(e)}")

    # 2. 嘗試 CSV
    try:
        resp = requests.get(url_csv, headers=COMMON_HEADERS, verify=False, timeout=15)
        if resp.status_code == 200 and not resp.text.strip().startswith('<'):
            # TWSE CSV 通常前幾行是標題，我們找包含 "證券代號" 的那一行
            lines = resp.text.split('\n')
            start_idx = 0
            for idx, line in enumerate(lines):
                if "證券代號" in line:
                    start_idx = idx
                    break
            df = pd.read_csv(io.StringIO("\n".join(lines[start_idx:])), on_bad_lines='skip')
            for _, row in df.iterrows():
                try:
                    ticker = str(row.iloc[1]).strip()
                    name = str(row.iloc[2]).strip()
                    period = str(row.iloc[8]).strip()
                    dates = re.findall(r'\d{3}/\d{2}/\d{2}', period)
                    end_date = dates[-1] if dates else "未知"
                    if ticker and len(ticker) >= 4:
                        results.append({"ticker": f"{ticker}.TW", "name": name, "end_date": end_date})
                except: continue
            if results: return results, ""
    except Exception as e:
        errors.append(f"TWSE CSV Error: {str(e)}")

    return results, "; ".join(errors)

def fetch_tpex_disposition():
    """ 抓取上櫃處置股 (優先 JSON, 失敗則 CSV) """
    print("Fetching TPEx (OTC) disposition stocks...")
    url_json = f"https://www.tpex.org.tw/web/bulletin/disposition/disposition_result.php?l=zh-tw&_={int(time.time()*1000)}"
    url_csv = "https://www.tpex.org.tw/web/bulletin/disposition/disposition_result.php?l=zh-tw&o=csv"
    
    results = []
    errors = []

    # 1. 嘗試 JSON
    try:
        resp = requests.get(url_json, headers=COMMON_HEADERS, verify=False, timeout=15)
        if resp.status_code == 200 and resp.text.strip():
            if resp.text.strip().startswith('<'):
                errors.append("TPEx JSON returned HTML (Blocked)")
            else:
                data = resp.json()
                if 'aaData' in data:
                    for item in data['aaData']:
                        ticker = item[0]
                        name = item[1]
                        period = item[2]
                        dates = re.findall(r'\d{3}/\d{2}/\d{2}', period)
                        end_date = dates[-1] if dates else "未知"
                        results.append({"ticker": f"{ticker}.TWO", "name": name, "end_date": end_date})
                    if results: return results, ""
        else:
            errors.append(f"TPEx JSON HTTP {resp.status_code}")
    except Exception as e:
        errors.append(f"TPEx JSON Error: {str(e)}")

    # 2. 嘗試 CSV
    try:
        resp = requests.get(url_csv, headers=COMMON_HEADERS, verify=False, timeout=15)
        if resp.status_code == 200 and not resp.text.strip().startswith('<'):
            # 上櫃 CSV 第一行通常是標題，我們找包含 "證券代號" 的那一行
            lines = resp.text.split('\n')
            start_idx = 0
            for idx, line in enumerate(lines):
                if "證券代號" in line:
                    start_idx = idx
                    break
            df = pd.read_csv(io.StringIO("\n".join(lines[start_idx:])), on_bad_lines='skip')
            for _, row in df.iterrows():
                try:
                    ticker = str(row.iloc[0]).strip()
                    name = str(row.iloc[1]).strip()
                    period = str(row.iloc[2]).strip()
                    dates = re.findall(r'\d{3}/\d{2}/\d{2}', period)
                    end_date = dates[-1] if dates else "未知"
                    if ticker and len(ticker) >= 4 and ticker.isdigit():
                        results.append({"ticker": f"{ticker}.TWO", "name": name, "end_date": end_date})
                except: continue
            if results: return results, ""
    except Exception as e:
        errors.append(f"TPEx CSV Error: {str(e)}")

    return results, "; ".join(errors)

def fetch_finmind_disposition():
    """ 透過 FinMind API 抓取處置股作為備援 """
    print("Fetching via FinMind API...")
    token = os.environ.get('FINMIND_TOKEN', '')
    try:
        from FinMind.data import DataLoader
        dl = DataLoader()
        if token:
            dl.login_by_token(token)
        
        # 抓取最近 10 天的資料，確保能抓到目前還在處置中的
        from datetime import timedelta
        start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
        df = dl.get_data(dataset='TaiwanStockDisposition', start_date=start_date)
        
        results = []
        if df is not None and not df.empty:
            # 轉換為統一格式
            for _, row in df.iterrows():
                ticker = str(row['stock_id']).strip()
                name = str(row['stock_name']).strip()
                # FinMind 的 end_date 格式通常是 YYYY-MM-DD
                raw_end = str(row['end_date']).strip()
                try:
                    dt = datetime.strptime(raw_end, '%Y-%m-%d')
                    end_date = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
                except:
                    end_date = raw_end
                
                # 簡單判斷上市上櫃 (這部分可能略有誤差，但作為備援已足夠)
                # 這裡暫時統一加 .TW，稍後在 main 中會進行處理
                results.append({"ticker": ticker, "name": name, "end_date": end_date})
        return results, ""
    except Exception as e:
        return [], f"FinMind Error: {str(e)}"

def fetch_fundamental_data(ticker):
    """ 抓取基本面數據 """
    try:
        tk = yf.Ticker(ticker)
        # 抓取現價與歷史 (1mo 資料足以計算現價與 MoM)
        hist = tk.history(period='1mo', timeout=10)
        if hist.empty:
            return None
        
        close_price = hist['Close'].iloc[-1]
        mom = 0.0
        if len(hist) >= 2:
            prev_price = hist['Close'].iloc[0]
            if prev_price > 0:
                mom = ((close_price - prev_price) / prev_price) * 100

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
    twse_stocks, twse_err = fetch_twse_disposition()
    tpex_stocks, tpex_err = fetch_tpex_disposition()
    fm_stocks, fm_err = fetch_finmind_disposition()
    
    # 彙整與去重
    results_map = {}
    
    # 處理上市資料
    for s in twse_stocks:
        results_map[s['ticker']] = s
        
    # 處理上櫃資料
    for s in tpex_stocks:
        results_map[s['ticker']] = s
        
    # 處理 FinMind 資料 (作為補充)
    import twstock
    for s in fm_stocks:
        ticker = s['ticker']
        # 判定後綴
        if ticker in twstock.codes:
            market = twstock.codes[ticker].market
            suffix = ".TW" if market == "上市" else ".TWO"
            full_ticker = f"{ticker}{suffix}"
            if full_ticker not in results_map:
                s['ticker'] = full_ticker
                results_map[full_ticker] = s

    error_msg = f"{twse_err} | {tpex_err} | {fm_err}".strip(" | ")
    final_list = list(results_map.values())
    
    print(f"Total unique stocks found: {len(final_list)}")
    
    enriched_results = []
    for s in final_list:
        print(f"Enriching {s['ticker']}...")
        fundamental = fetch_fundamental_data(s['ticker'])
        if fundamental:
            s.update(fundamental)
        else:
            s.update({"close": 0, "pb": 0, "yoy": 0, "mom": 0, "industry": "未知"})
        enriched_results.append(s)
        time.sleep(random.uniform(0.5, 1.2))

    today_str = datetime.now().strftime('%Y-%m-%d')
    output = {
        "scan_date": today_str,
        "data_date": today_str,
        "error_msg": error_msg if not enriched_results else "",
        "data": enriched_results
    }
    
    with open('disposition_data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    
    print(f"Saved {len(enriched_results)} stocks to disposition_data.json. Error log: {output['error_msg']}")

if __name__ == "__main__":
    main()
