import yfinance as yf
import pandas as pd
import json
import os
import twstock
import time
import random
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定檔案路徑
CHECKPOINT_FILE = './.scan_checkpoint.json'
OUTPUT_FILE = './stock_data.json'

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
    
    # 即時更新輸出檔案，採用新結構
    output_dict = {
        "scan_date": today_str,
        "data_date": today_str,
        "empty_today": False,
        "data": results
    }
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_dict, f, ensure_ascii=False, indent=4)

def fetch_single_ticker(ticker, codes_dict, history_counts, is_today):
    """ 抓取單一標的完整數據 """
    for attempt in range(2):
        try:
            tk = yf.Ticker(ticker)

            # 1. 抓取歷史資料 (2年長度用以計算SMA200與120高)
            df_daily = tk.history(period='2y', timeout=5)

            if df_daily.empty or 'Close' not in df_daily.columns:
                return None

            # 2. 抓取月線資料 (計算 MoM)
            df_monthly = tk.history(period='2mo', interval='1mo', timeout=5)
            has_monthly = not df_monthly.empty and 'Close' in df_monthly.columns

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
            # 120日高點濾網 (過去 120 個交易日的最高價，不含今日則用 shift(1))
            high120 = hist['High'].rolling(window=120).max().shift(1).iloc[-1]

            # --- 10倍飆股策略核心過濾條件 ---
            # 1. 站上長均線：Close > 200MA
            cond1 = last_close > sma200
            # 2. 突破新高：Close >= 120日高點
            cond2 = last_close >= high120
            # 3. 均線糾結濾網：10MA/200MA < 2
            ratio = sma10 / sma200 if sma200 > 0 else 0
            cond3 = ratio < 2.0

            if not (cond1 and cond2 and cond3):
                return None

            # 月報酬 (Price MoM%)
            mom = 0.0
            if has_monthly:
                hist_mo = df_monthly.dropna(subset=['Close'])
                if len(hist_mo) >= 2:
                    current_mo = hist_mo['Close'].iloc[-1]
                    prev_mo = hist_mo['Close'].iloc[-2]
                    if prev_mo > 0:
                        mom = ((current_mo - prev_mo) / prev_mo) * 100

            # 3. 抓取基本面 info
            info = tk.info

            def clean(val, default=0):
                try:
                    return round(float(val), 2) if pd.notnull(val) and val is not None else default
                except:
                    return default

            res = {
                "ticker": ticker,
                "name": name,
                "close": clean(last_close),
                "sma200": clean(sma200),
                "high120": clean(high120),
                "ratio": clean(ratio),
                "pb": clean(info.get('priceToBook')),
                "eps": clean(info.get('trailingEps')),
                "yoy": clean(info.get('revenueGrowth', 0) * 100),
                "mom": clean(mom),
                "industry": industry,
                "consecutive_days": history_counts.get(ticker, 1) if is_today else history_counts.get(ticker, 0) + 1
            }

            # 禮貌延遲，避免過快被封鎖
            time.sleep(random.uniform(1.0, 2.5))
            return res

        except Exception as e:
            err_msg = str(e)
            if "Too Many Requests" in err_msg or "429" in err_msg:
                print(f"[{ticker}] 觸發 Rate Limit! 暫停 60 秒... (嘗試 {attempt+1}/2)")
                if attempt == 0:
                    time.sleep(60)
                    continue
            else:
                print(f"[{ticker}] 抓取錯誤: {err_msg}")
            break # 非 429 錯誤或第二次嘗試失敗則跳出

    return None

def run_robust_scanner():
    all_tickers = get_all_taiwan_tickers()
    codes_dict = twstock.codes
    checkpoint = load_checkpoint()
    
    done_tickers_set = set(checkpoint["done_tickers"])
    results = checkpoint["results"]
    
    # 載入歷史天數邏輯
    history_counts = {}
    is_today = False
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                old_json = json.load(f)
                
                # 支援新舊結構
                if isinstance(old_json, dict):
                    old_data = old_json.get("data", [])
                    last_run_date = old_json.get("scan_date") or old_json.get("last_run", "")
                else:
                    old_data = old_json
                    last_run_date = ""

                today_date = datetime.now().strftime('%Y-%m-%d')
                is_today = (last_run_date == today_date)
                for item in old_data:
                    t = item.get('ticker')
                    if t: history_counts[t] = item.get('consecutive_days', 1)
        except: pass

    tickers_to_scan = [t for t in all_tickers if t not in done_tickers_set]
    print(f"[ENGINE-V3.2] Starting Scanner. Remaining: {len(tickers_to_scan)}/{len(all_tickers)}")

    # 分批處理
    chunk_size = 30
    for i in range(0, len(tickers_to_scan), chunk_size):
        chunk = tickers_to_scan[i:i + chunk_size]
        
        # 併發執行 (限制 max_workers 以降低請求強度)
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_ticker = {executor.submit(fetch_single_ticker, t, codes_dict, history_counts, is_today): t for t in chunk}
            
            for future in tqdm(as_completed(future_to_ticker), total=len(chunk), desc=f"批次 {i//chunk_size + 1}"):
                ticker = future_to_ticker[future]
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                    done_tickers_set.add(ticker)
                except Exception as e:
                    print(f"[UNEXPECTED] {ticker}: {e}")
        
        # 每一大批次完成後存檔
        save_checkpoint(list(done_tickers_set), results)
        print(f"Progress Saved: {len(done_tickers_set)} tickers processed.")
        time.sleep(5) # 批次間休息

    # 最終落地方案：如果最後一刻還是沒抓到任何資料（results是空的）
    if len(results) == 0:
        print("[警告] 本次全掃描結果為空，正在嘗試載入現有資料備份...")
        if os.path.exists(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    old_json = json.load(f)
                    today_str = datetime.now().strftime('%Y-%m-%d')
                    
                    if isinstance(old_json, dict):
                        old_data = old_json.get("data", [])
                        old_data_date = old_json.get("data_date") or old_json.get("last_run") or "未知"
                    else:
                        old_data = old_json
                        old_data_date = "未知"
                    
                    final_output = {
                        "scan_date": today_str,
                        "data_date": old_data_date,
                        "empty_today": True,
                        "data": old_data
                    }
                    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                        json.dump(final_output, f, ensure_ascii=False, indent=4)
                    print(f"[系統] 已更新 scan_date，且今日無標的 (empty_today: true)，維持顯示 {old_data_date} 的資料。")
            except Exception as e:
                print(f"[錯誤] 讀取備份資料失敗: {e}")
    else:
        print(f"\n--- [COMPLETED] Final Results: {len(results)} ---")

    # 清除暫存進度
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

if __name__ == "__main__":
    run_robust_scanner()
