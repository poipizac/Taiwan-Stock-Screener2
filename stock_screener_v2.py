
import yfinance as yf
import pandas as pd
import json
import os
import twstock
import time
import random
import random
from datetime import datetime
from tqdm import tqdm

# 設定檔案路徑
CHECKPOINT_FILE = 'd:/Antigravity/Stock_finder/.scan_checkpoint.json'
OUTPUT_FILE = 'd:/Antigravity/Stock_finder/stock_data.json'

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
    """ 持續存檔進度 (支援雲端記憶結構) """
    today_str = datetime.now().strftime('%Y-%m-%d')
    checkpoint = {"done_tickers": done_tickers, "results": results}
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=4)
    # 同步更新正式輸出 JSON (採用新結構)
    output_dict = {
        "last_run": today_str,
        "data": results
    }
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_dict, f, ensure_ascii=False, indent=4)


def process_batch(batch_data, monthly_data, tickers, codes_dict, history_counts=None, is_today=False):
    """ 處理技術面、月報酬 (Price MoM) 與基本面數據 """
    if history_counts is None:
        history_counts = {}
    batch_results = []
    for ticker in tickers:
        try:
            if ticker not in batch_data.columns.levels[0]:
                continue
                
            hist = batch_data[ticker].dropna(subset=['Close'])
            if len(hist) < 200:
                continue
                
            stock_id = ticker.split('.')[0]
            name = codes_dict[stock_id].name if stock_id in codes_dict else ticker
            industry = codes_dict[stock_id].group if stock_id in codes_dict else '未知'
            
            # 1. 技術面指標 (SMA10, SMA200, 120D High)
            last_close = hist['Close'].iloc[-1]
            sma10 = hist['Close'].rolling(window=10).mean().iloc[-1]
            sma200 = hist['Close'].rolling(window=200).mean().iloc[-1]
            high120 = hist['High'].rolling(window=120).max().shift(1).iloc[-1]
            
            # 2. 股價月報酬 (Price MoM%)
            mom = 0.0
            if ticker in monthly_data.columns.levels[0]:
                hist_mo = monthly_data[ticker].dropna(subset=['Close'])
                if len(hist_mo) >= 2:
                    current_mo = hist_mo['Close'].iloc[-1]
                    prev_mo = hist_mo['Close'].iloc[-2]
                    if prev_mo > 0:
                        mom = ((current_mo - prev_mo) / prev_mo) * 100
            
            # 3. 基本面數據 (獲取 Ticker 物件 info)
            tk = yf.Ticker(ticker)
            info = tk.info if tk else {}
            
            def clean(val, default=0):
                return round(float(val), 2) if pd.notnull(val) and val is not None else default

            batch_results.append({
                "ticker": ticker,
                "name": name,
                "close": clean(last_close),
                "sma200": clean(sma200),
                "high120": clean(high120),
                "ratio": clean(sma10 / sma200 if sma200 > 0 else 0),
                "pb": clean(info.get('priceToBook')),
                "eps": clean(info.get('trailingEps')),
                "yoy": clean(info.get('revenueGrowth', 0) * 100),
                "mom": clean(mom),  # 儲存股價月報酬
                "industry": industry,
                "consecutive_days": history_counts.get(ticker, 1) if is_today else history_counts.get(ticker, 0) + 1
            })
        except Exception:
            continue
    return batch_results

def run_robust_scanner():
    all_tickers = get_all_taiwan_tickers()
    codes_dict = twstock.codes
    checkpoint = load_checkpoint()
    
    done_tickers_set = set(checkpoint["done_tickers"])
    results = checkpoint["results"]
    
    # --- 新增：載入歷史進榜天數 ---
    history_counts = {}
    is_today = False
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                old_json = json.load(f)
                
                # 相容舊版陣列與新版字典結構
                if isinstance(old_json, dict):
                    last_run_date = old_json.get("last_run", "")
                    old_data = old_json.get("data", [])
                else:
                    # 舊版陣列：回退到檔案修改日期
                    mtime = os.path.getmtime(OUTPUT_FILE)
                    last_run_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
                    old_data = old_json

                today_date = datetime.now().strftime('%Y-%m-%d')
                is_today = (last_run_date == today_date)

                for item in old_data:
                    t = item.get('ticker')
                    days = item.get('consecutive_days', 1)
                    if t:
                        history_counts[t] = days
        except Exception as e:
            print(f"[HISTORY] Load failed/Empty: {e}")
    # ----------------------------
    
    total_count = len(all_tickers)
    print(f"[INFO] Initializing scan engine... [Currently: {len(done_tickers_set)}/{total_count}]")

    batch_size = 50 
    for i in range(0, total_count, batch_size):
        batch = all_tickers[i:i + batch_size]
        batch_to_do = [t for t in batch if t not in done_tickers_set]
        if not batch_to_do:
            continue
            
        print(f"\n[INFO] Processing: {i+1}~{min(i+batch_size, total_count)} / {total_count}")
        
        try:
            # 1. 批次技術面下載 (日線與月線)
            data_daily = yf.download(batch_to_do, period='2y', group_by='ticker', threads=True, progress=False)
            data_monthly = yf.download(batch_to_do, period='2mo', interval='1mo', group_by='ticker', threads=True, progress=False)
            
            if not data_daily.empty:
                batch_res = process_batch(data_daily, data_monthly, batch_to_do, codes_dict, history_counts, is_today)
                results.extend(batch_res)
                for t in batch_to_do:
                    done_tickers_set.add(t)
            
            # 定期寫入斷點，確保進度持久化
            save_checkpoint(list(done_tickers_set), results)
            print(f"[INFO] Progress saved. Current valid count: {len(results)}")
            
            # 平衡速度與穩定性：避免 YFRateLimitError
            time.sleep(1.0)
            
        except Exception as e:
            print(f"[ERROR] Batch failed: {e}. Skipping to next...")
            continue

    print(f"\n--- [INFO] Scan Completed ---")
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

if __name__ == "__main__":
    run_robust_scanner()
