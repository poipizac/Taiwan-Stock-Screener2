import yfinance as yf
import pandas as pd
import json
import os
import twstock
import time
import random
import requests
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定檔案路徑
CHECKPOINT_FILE = './.scan_checkpoint.json'
OUTPUT_FILE = './stock_data.json'

# --- 核心修改 1: 偽裝真實瀏覽器 Session ---
def get_session():
    session = requests.Session()
    # 使用常見的 Windows Chrome User-Agent
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Connection': 'keep-alive',
    })
    return session

def get_all_taiwan_tickers():
    """ 獲取所有上市與上櫃股票代號並加上正確後綴 """
    codes = twstock.codes
    tickers = []
    for code, info in codes.items():
        if info.type == '股票' and info.market in ['上市', '上櫃'] and len(code) == 4:
            suffix = '.TW' if info.market == '上市' else '.TWO'
            tickers.append(f"{code}{suffix}")
    return sorted(tickers)

def load_checkpoint():
    """ 載入斷點續傳進度 """
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except:
            return {"done_tickers": [], "results": []}
    return {"done_tickers": [], "results": []}

def save_checkpoint(done_tickers, results):
    """ 持續存檔進度 """
    today_str = datetime.now().strftime('%Y-%m-%d')
    checkpoint = {"done_tickers": done_tickers, "results": results}
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=4)
    output_dict = {"last_run": today_str, "data": results}
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_dict, f, ensure_ascii=False, indent=4)

# --- 核心修改 2 & 3: 單檔抓取、偽裝 Session 與 3s 強制超時 ---
def fetch_single_ticker(ticker, session, codes_dict, history_counts, is_today):
    """ 抓取單一標的完整數據，失敗立刻返回 None 絕不卡死 """
    try:
        # 1. 抓取歷史資料 (強制 3 秒超時)
        # 使用 yf.download 抓取單檔
        df_daily = yf.download(ticker, period='2y', session=session, progress=False, ignore_tz=True, timeout=3)
        if df_daily.empty:
            return None
            
        # 2. 抓取月線資料 (計算 MoM)
        df_monthly = yf.download(ticker, period='2mo', interval='1mo', session=session, progress=False, ignore_tz=True, timeout=3)
        
        hist = df_daily.dropna(subset=['Close'])
        if len(hist) < 200:
            return None
            
        stock_id = ticker.split('.')[0]
        name = codes_dict[stock_id].name if stock_id in codes_dict else ticker
        industry = codes_dict[stock_id].group if stock_id in codes_dict else '未知'
        
        # 指標計算
        last_close = hist['Close'].iloc[-1]
        sma10 = hist['Close'].rolling(window=10).mean().iloc[-1]
        sma200 = hist['Close'].rolling(window=200).mean().iloc[-1]
        high120 = hist['High'].rolling(window=120).max().shift(1).iloc[-1]
        
        # 月報酬
        mom = 0.0
        if not df_monthly.empty:
            hist_mo = df_monthly.dropna(subset=['Close'])
            if len(hist_mo) >= 2:
                current_mo = hist_mo['Close'].iloc[-1]
                prev_mo = hist_mo['Close'].iloc[-2]
                if prev_mo > 0:
                    mom = ((current_mo - prev_mo) / prev_mo) * 100
        
        # 3. 抓取基本面 info
        tk = yf.Ticker(ticker, session=session)
        # yfinance 的 info 屬性會發起網路請求，這也是常見卡死點
        info = tk.info
        
        def clean(val, default=0):
            return round(float(val), 2) if pd.notnull(val) and val is not None else default

        return {
            "ticker": ticker,
            "name": name,
            "close": clean(last_close),
            "sma200": clean(sma200),
            "high120": clean(high120),
            "ratio": clean(sma10 / sma200 if sma200 > 0 else 0),
            "pb": clean(info.get('priceToBook')),
            "eps": clean(info.get('trailingEps')),
            "yoy": clean(info.get('revenueGrowth', 0) * 100),
            "mom": clean(mom),
            "industry": industry,
            "consecutive_days": history_counts.get(ticker, 1) if is_today else history_counts.get(ticker, 0) + 1
        }
    except Exception as e:
        # 核心優化：印出具體錯誤原因以便偵測
        print(f"[{ticker}] 抓取失敗: {e}")
        return None

def run_robust_scanner():
    all_tickers = get_all_taiwan_tickers()
    codes_dict = twstock.codes
    checkpoint = load_checkpoint()
    
    done_tickers_set = set(checkpoint["done_tickers"])
    results = checkpoint["results"]
    
    # 載入歷史天數
    history_counts = {}
    is_today = False
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                old_json = json.load(f)
                old_data = old_json.get("data", []) if isinstance(old_json, dict) else old_json
                last_run_date = old_json.get("last_run", "") if isinstance(old_json, dict) else ""
                today_date = datetime.now().strftime('%Y-%m-%d')
                is_today = (last_run_date == today_date)
                for item in old_data:
                    t = item.get('ticker')
                    if t: history_counts[t] = item.get('consecutive_days', 1)
        except: pass

    # 過濾已完成
    tickers_to_scan = [t for t in all_tickers if t not in done_tickers_set]
    total_to_scan = len(tickers_to_scan)
    print(f"[ENGINE-V3] Starting Overhaul Scanner. Remaining: {total_to_scan}/{len(all_tickers)}")

    # 核心修改 4: 使用 ThreadPoolExecutor 併發處理
    session = get_session()
    
    # 每次處理 20 個標的就存一次檔
    chunk_size = 20
    for i in range(0, len(tickers_to_scan), chunk_size):
        chunk = tickers_to_scan[i:i + chunk_size]
        
        # 併發執行單檔抓取 (max_workers 設為 6，適中不激進)
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_to_ticker = {executor.submit(fetch_single_ticker, t, session, codes_dict, history_counts, is_today): t for t in chunk}
            
            for future in tqdm(as_completed(future_to_ticker), total=len(chunk), desc=f"Chunk {i//chunk_size + 1}"):
                ticker = future_to_ticker[future]
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                    done_tickers_set.add(ticker)
                except Exception as e:
                    print(f"[FUTURE-ERROR] {ticker}: {e}")
        
        # 每組完成後存檔並稍作休息
        save_checkpoint(list(done_tickers_set), results)
        time.sleep(random.uniform(1.0, 2.0))

    print(f"\n--- [COMPLETED] Final Results: {len(results)} ---")
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

if __name__ == "__main__":
    run_robust_scanner()
