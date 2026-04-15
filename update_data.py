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
    output_dict = {"last_run": today_str, "data": results}
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_dict, f, ensure_ascii=False, indent=4)

# --- 核心修改：移除 Session，交由 YF 自行處理 TLS 指紋 ---
def fetch_single_ticker(ticker, codes_dict, history_counts, is_today):
    """ 抓取單一標的完整數據，失敗立刻返回 None 絕不卡死 """
    try:
        tk = yf.Ticker(ticker)
        
        # 1. 抓取歷史資料 (改用 tk.history 保證回傳扁平的 DataFrame)
        df_daily = tk.history(period='2y', timeout=3)
        
        # 防呆檢查：如果 df 為空或缺少必要欄位 'Close'
        if df_daily.empty or 'Close' not in df_daily.columns:
            return None
            
        # 2. 抓取月線資料 (計算 MoM)
        df_monthly = tk.history(period='2mo', interval='1mo', timeout=3)
        
        # 即使月線抓取失敗，我們仍可繼續處理日線資料，只需給予 MoM 預設值
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
        high120 = hist['High'].rolling(window=120).max().shift(1).iloc[-1]
        
        # 月報酬 (Price MoM%)
        mom = 0.0
        if has_monthly:
            # 確保提取時不會報錯
            hist_mo = df_monthly.dropna(subset=['Close'])
            if len(hist_mo) >= 2:
                current_mo = hist_mo['Close'].iloc[-1]
                prev_mo = hist_mo['Close'].iloc[-2]
                if prev_mo > 0:
                    mom = ((current_mo - prev_mo) / prev_mo) * 100
        
        # 3. 抓取基本面 info
        # yfinance 的 info 屬性會發起網路請求
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
        # 印出具體錯誤原因以便偵測 (保留防護網)
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
    print(f"[ENGINE-V3.1] Starting Optimized Scanner (No Session). Remaining: {total_to_scan}/{len(all_tickers)}")

    # 併發處理 (不使用自訂 session)
    chunk_size = 20
    for i in range(0, len(tickers_to_scan), chunk_size):
        chunk = tickers_to_scan[i:i + chunk_size]
        
        # 併發執行單檔抓取 (max_workers 設為 6)
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_to_ticker = {executor.submit(fetch_single_ticker, t, codes_dict, history_counts, is_today): t for t in chunk}
            
            for future in tqdm(as_completed(future_to_ticker), total=len(chunk), desc=f"Chunk {i//chunk_size + 1}"):
                ticker = future_to_ticker[future]
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                    done_tickers_set.add(ticker)
                except Exception as e:
                    print(f"[FUTURE-ERROR] {ticker}: {e}")
        
        # 每組完成後存檔並稍作休息 (禮貌性延遲)
        save_checkpoint(list(done_tickers_set), results)
        time.sleep(random.uniform(1.0, 2.0))

    print(f"\n--- [COMPLETED] Final Results: {len(results)} ---")
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

if __name__ == "__main__":
    run_robust_scanner()
