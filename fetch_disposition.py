import requests
import json
import pandas as pd
import yfinance as yf
import twstock
import re
import time
import random
import os
from datetime import datetime, timedelta

# 禁用 SSL 警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 極度擬真的 Chrome 瀏覽器 Headers
CHROME_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="125", "Google Chrome";v="125", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

OUTPUT_FILE = './disposition_data.json'


def roc_date_to_datetime(roc_str):
    """
    將民國日期字串 (e.g. '115/05/20') 轉換為 datetime 物件。
    回傳 None 代表解析失敗。
    """
    try:
        parts = roc_str.strip().split('/')
        y = int(parts[0]) + 1911
        m = int(parts[1])
        d = int(parts[2])
        return datetime(y, m, d)
    except:
        return None


def fetch_twse_disposition():
    """
    抓取上市處置股。
    API: https://www.twse.com.tw/rwd/zh/announcement/punish?response=json&startDate=YYYYMMDD&endDate=YYYYMMDD
    欄位 (fields): [編號, 公布日期, 證券代號, 證券名稱, 累計, 處置條件, 處置起迄時間, 處置措施, 處置內容, 備註]
    索引: [2]=證券代號, [3]=名稱, [6]=處置起迄時間 (格式: "115/04/17～115/04/30")
    """
    print("[TWSE] 開始抓取上市處置股...")
    today = datetime.now()
    start_date = (today - timedelta(days=60)).strftime('%Y%m%d')
    end_date = (today + timedelta(days=30)).strftime('%Y%m%d')

    url = f"https://www.twse.com.tw/rwd/zh/announcement/punish?response=json&startDate={start_date}&endDate={end_date}"
    headers = {**CHROME_HEADERS, "Referer": "https://www.twse.com.tw/zh/announcement/punish.html"}

    for retry in range(3):
        try:
            if retry > 0:
                wait = 10 * retry
                print(f"[TWSE] 第 {retry+1} 次嘗試 (等待 {wait} 秒)...")
                time.sleep(wait)

            resp = requests.get(url, headers=headers, verify=False, timeout=20)
            print(f"[TWSE] HTTP Status: {resp.status_code}")

            if resp.status_code != 200:
                continue

            text = resp.text.strip()
            if text.startswith('<'):
                print("[TWSE] 收到 HTML 回應 (可能被暫時封鎖)")
                continue

            data = resp.json()
            if data.get('stat') != 'OK' or 'data' not in data or not data['data']:
                return [], "TWSE returned empty data (可能當前無上市處置股)"

            # 成功取得資料，跳出重試迴圈
            break
        except Exception as e:
            if retry == 2:
                return [], f"TWSE Error: {str(e)}"
            continue
    else:
        return [], "IP Blocked by TWSE (3 次嘗試皆失敗)"

    today_midnight = today.replace(hour=0, minute=0, second=0, microsecond=0)

    # 逐筆解析，取每支股票「最晚的處置迄日」
    stock_map = {}  # ticker_code -> {"name": ..., "end_date_roc": ..., "end_dt": datetime}
    for item in data['data']:
        try:
            code = str(item[2]).strip()
            name = str(item[3]).strip()
            period_str = str(item[6]).strip()

            # 只保留 4 碼股票代號 (排除權證等 6 碼代號)
            if not (len(code) == 4 and code.isdigit()):
                continue

            # 解析處置期間："115/04/17～115/04/30"
            parts = re.split(r'[～~]', period_str)
            if len(parts) < 2:
                continue

            end_date_roc = parts[-1].strip()
            end_dt = roc_date_to_datetime(end_date_roc)
            if end_dt is None:
                continue

            # 過濾已解除處置 (處置迄日 < 今天)
            if end_dt < today_midnight:
                continue

            # 同一支股票保留最晚的處置迄日
            if code not in stock_map or end_dt > stock_map[code]['end_dt']:
                stock_map[code] = {
                    "name": name,
                    "end_date_roc": end_date_roc,
                    "end_dt": end_dt
                }
        except Exception:
            continue

    results = []
    for code, info in stock_map.items():
        # 使用 twstock 取得更完整的名稱
        official_name = twstock.codes[code].name if code in twstock.codes else info['name']
        results.append({
            "ticker": f"{code}.TW",
            "name": official_name,
            "end_date": info['end_date_roc']
        })

    print(f"[TWSE] 抓取完成，共 {len(results)} 檔上市處置股")
    return results, ""


def fetch_tpex_disposition():
    """
    抓取上櫃處置股。
    API: https://www.tpex.org.tw/www/zh-tw/bulletin/disposal?response=json&startDate=YYY/MM/DD&endDate=YYY/MM/DD
    回傳結構: { "tables": [{ "data": [[...], ...] }] }
    索引: [2]=證券代號, [3]=名稱, [5]=處置起迄時間 (格式: "115/05/08~115/05/21")
    """
    print("[TPEx] 開始抓取上櫃處置股...")
    today = datetime.now()
    start = today - timedelta(days=60)
    end = today + timedelta(days=30)
    start_roc = f"{start.year - 1911}/{start.month:02d}/{start.day:02d}"
    end_roc = f"{end.year - 1911}/{end.month:02d}/{end.day:02d}"

    url = f"https://www.tpex.org.tw/www/zh-tw/bulletin/disposal?response=json&startDate={start_roc}&endDate={end_roc}"
    headers = {**CHROME_HEADERS, "Referer": "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"}

    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=20)
        if resp.status_code != 200:
            return [], f"TPEx HTTP {resp.status_code}"

        text = resp.text.strip()
        if text.startswith('<'):
            return [], "IP Blocked by TPEx"

        data = resp.json()
        tables = data.get('tables', [])
        if not tables or 'data' not in tables[0] or not tables[0]['data']:
            return [], "TPEx returned empty data (可能當前無上櫃處置股)"

        today_midnight = today.replace(hour=0, minute=0, second=0, microsecond=0)
        rows = tables[0]['data']

        stock_map = {}
        for item in rows:
            try:
                code = str(item[2]).strip()
                name = str(item[3]).strip()
                period_str = str(item[5]).strip()

                if not (len(code) == 4 and code.isdigit()):
                    continue

                parts = re.split(r'[～~]', period_str)
                if len(parts) < 2:
                    continue

                end_date_roc = parts[-1].strip()
                end_dt = roc_date_to_datetime(end_date_roc)
                if end_dt is None:
                    continue

                if end_dt < today_midnight:
                    continue

                if code not in stock_map or end_dt > stock_map[code]['end_dt']:
                    stock_map[code] = {
                        "name": name,
                        "end_date_roc": end_date_roc,
                        "end_dt": end_dt
                    }
            except Exception:
                continue

        results = []
        for code, info in stock_map.items():
            official_name = twstock.codes[code].name if code in twstock.codes else info['name']
            results.append({
                "ticker": f"{code}.TWO",
                "name": official_name,
                "end_date": info['end_date_roc']
            })

        print(f"[TPEx] 抓取完成，共 {len(results)} 檔上櫃處置股")
        return results, ""

    except Exception as e:
        return [], f"TPEx Error: {str(e)}"


def fetch_fundamental_data(ticker):
    """ 使用 yfinance 抓取基本面數據 (現價、PB、YoY%、MoM%) """
    for attempt in range(2):
        try:
            tk = yf.Ticker(ticker)
            hist = tk.history(period='1mo', timeout=5)
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
                    if val is None or pd.isna(val):
                        return default
                    return round(float(val), 2)
                except:
                    return default

            # 產業分類使用 twstock 的繁體中文分類
            stock_id = ticker.split('.')[0]
            industry_zh = twstock.codes[stock_id].group if stock_id in twstock.codes else '未知'

            return {
                "close": clean(close_price),
                "pb": clean(info.get('priceToBook')),
                "yoy": clean(info.get('revenueGrowth', 0) * 100),
                "mom": clean(mom),
                "industry": industry_zh
            }
        except Exception as e:
            err = str(e)
            if "Too Many Requests" in err or "429" in err:
                print(f"  [{ticker}] Rate limited, 等待 30 秒... (嘗試 {attempt+1}/2)")
                time.sleep(30)
                continue
            print(f"  [{ticker}] yfinance 錯誤: {err}")
            break
    return None


def main():
    print("=" * 60)
    print(f"[處置股爬蟲] 啟動時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    twse_stocks, twse_err = fetch_twse_disposition()
    time.sleep(3)  # 兩個交易所之間間隔
    tpex_stocks, tpex_err = fetch_tpex_disposition()

    all_stocks = twse_stocks + tpex_stocks
    all_errors = [e for e in [twse_err, tpex_err] if e]
    error_msg = " | ".join(all_errors)

    print(f"\n[彙總] 共找到 {len(all_stocks)} 檔處置股 (上市 {len(twse_stocks)} + 上櫃 {len(tpex_stocks)})")

    # 補齊基本面資訊
    enriched = []
    for s in all_stocks:
        print(f"  補齊基本面: {s['ticker']} ({s['name']})...")
        fundamental = fetch_fundamental_data(s['ticker'])
        if fundamental:
            s.update(fundamental)
        else:
            s.update({"close": 0, "pb": 0, "yoy": 0, "mom": 0, "industry": "未知"})
        enriched.append(s)
        time.sleep(random.uniform(0.5, 1.5))

    today_str = datetime.now().strftime('%Y-%m-%d')

    # 判斷最終錯誤訊息
    if not enriched and error_msg:
        final_error = error_msg
    elif not enriched and not error_msg:
        final_error = "當前無任何處置中之有價證券"
    else:
        final_error = ""

    output = {
        "scan_date": today_str,
        "data_date": today_str,
        "error_msg": final_error,
        "data": enriched
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    print(f"\n[完成] 儲存 {len(enriched)} 檔至 {OUTPUT_FILE}")
    if final_error:
        print(f"[錯誤記錄] {final_error}")


if __name__ == "__main__":
    main()
