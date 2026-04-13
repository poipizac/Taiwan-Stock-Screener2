import yfinance as yf
import pandas as pd
import json
import os
import time
import warnings
from datetime import datetime, timedelta
from tqdm import tqdm
from FinMind.data import DataLoader
import twstock

# 1. 初始化與設定
warnings.filterwarnings("ignore")
DL = DataLoader()

# 若環境中有 FinMind Token 則登入
FINMIND_TOKEN = os.getenv('FINMIND_TOKEN')
if FINMIND_TOKEN:
    try:
        DL.login(token=FINMIND_TOKEN)
        print("[系統] 已成功登入 FinMind Token。")
    except Exception as e:
        print(f"[警告] FinMind 登入失敗: {e}")

def load_tickers_from_json(file_path):
    """ 從 JSON 檔案讀取 Tickers (支援新版 dict 結構) """
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
                # 判斷是新版 dict {"data": [...]} 還是舊版 list [...]
                data = content.get("data", []) if isinstance(content, dict) else content
                return [item['ticker'] for item in data if 'ticker' in item]
        except Exception as e:
            print(f"[警告] 讀取 JSON 失敗: {e}")
    return []

def get_stage1_candidates(tickers):
    """
    第一層漏斗：技術與基本面初篩 (yfinance)
    - 收盤價 > 5MA
    - 今日成交量 > 過去 5 日平均量 * 2
    - 營收 YoY > 20%
    """
    candidates = []
    print(f"[Step 1] 開始 yfinance 技術與基本面初篩 (總數: {len(tickers)})...")
    
    for ticker in tqdm(tickers, desc="初篩進度"):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period='1mo')
            
            if df.empty or len(df) < 6:
                continue
            
            # 技術指標計算
            df['5MA'] = df['Close'].rolling(window=5).mean()
            avg_vol_5d = df['Volume'].shift(1).rolling(window=5).mean().iloc[-1]
            
            today_close = df['Close'].iloc[-1]
            today_vol = df['Volume'].iloc[-1]
            ma5_val = df['5MA'].iloc[-1]
            
            # 初篩條件 A & B
            if not (today_close > ma5_val and today_vol > (avg_vol_5d * 2)):
                continue
            
            # 初篩條件 C：營收 YoY > 20%
            info = stock.info
            rev_growth = info.get('revenueGrowth', 0)
            if rev_growth is None: rev_growth = 0
            
            if rev_growth > 0.2:
                # 獲取中文名稱 (twstock 優先，yfinance 為輔)
                stock_id = ticker.split('.')[0]
                tw_info = twstock.codes.get(stock_id)
                ch_name = tw_info.name if tw_info else info.get('shortName', ticker)

                candidates.append({
                    'ticker': ticker,
                    'stock_id': stock_id,
                    'name': ch_name,
                    'price': round(today_close, 2),
                    'volume': int(today_vol),
                    'avg_vol_5d': round(avg_vol_5d, 0),
                    'yoy': f"{round(rev_growth * 100, 2)}%"
                })
        except Exception:
            continue
            
    return candidates

def filter_stage2_institutional(candidates):
    """
    第二層漏斗：籌碼面過濾 (FinMind)
    - 只有通過第一層的股票才進入此處
    - 條件：近 3 日「投信」總買賣超 > 0
    """
    final_picks = []
    print(f"\n[Step 2] 開始 FinMind 籌碼面過濾 (初篩入選: {len(candidates)})...")
    
    # 計算起始日期 (取近 10 天確保涵蓋 3 個交易日)
    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    
    for item in candidates:
        stock_id = item['stock_id']
        try:
            # 禮貌延時
            time.sleep(1)
            
            # 抓取法人買賣超數據
            df_inst = DL.taiwan_stock_institutional_investors(
                stock_id=stock_id,
                start_date=start_date
            )
            
            if df_inst.empty:
                continue
            
            # 過濾出「投信 (Investment_Trust)」的資料
            it_data = df_inst[df_inst['name'] == 'Investment_Trust'].tail(3)
            
            if it_data.empty:
                continue
                
            # 計算近 3 日總買賣超 (張數 = buy - sell)
            # 注意：FinMind 回傳通常是股數，需除以 1000 轉換為張
            net_buy_shares = it_data['buy'].sum() - it_data['sell'].sum()
            net_buy_lot = round(net_buy_shares / 1000, 1)
            
            # 籌碼條件：投信總和為買超
            if net_buy_shares > 0:
                item['投信近3日買賣超(張)'] = net_buy_lot
                final_picks.append(item)
                
        except Exception as e:
            # print(f"[警告] 處理 {stock_id} 籌碼出錯: {e}")
            continue
            
    return final_picks

if __name__ == "__main__":
    # JSON 路徑
    JSON_PATH = r'd:\Antigravity\Stock_finder\stock_data.json'
    
    # 1. 讀取標的
    all_tickers = load_tickers_from_json(JSON_PATH)
    if not all_tickers:
        print("[資訊] 使用自定義清單。")
        all_tickers = ['2330.TW', '2317.TW', '2454.TW', '3231.TW', '2603.TW', '2609.TW']
        
    # 2. 第一階段漏斗 (yfinance)
    stage1_passers = get_stage1_candidates(all_tickers)
    
    # 3. 第二階段漏斗 (FinMind)
    if stage1_passers:
        final_results = filter_stage2_institutional(stage1_passers)
    else:
        final_results = []
        print("\n[結果] 無標的通過第一階段技術面漏斗。")
    
    # 4. 輸出最終結果
    print("\n" + "★"*20 + " 終極飆股篩選報告 " + "★"*20)
    if final_results:
        # 儲存至 JSON 檔案供網頁前端使用
        MOMENTUM_OUTPUT = 'momentum_data.json'
        
        # --- 新增：處理連續進榜天數 (雲端記憶版) ---
        consecutive_map = {}
        is_today = False
        try:
            if os.path.exists(MOMENTUM_OUTPUT):
                with open(MOMENTUM_OUTPUT, 'r', encoding='utf-8') as f:
                    old_json = json.load(f)
                    
                    # 相容新舊結構
                    if isinstance(old_json, dict):
                        last_run_date = old_json.get("last_run", "")
                        old_data = old_json.get("data", [])
                    else:
                        mtime = os.path.getmtime(MOMENTUM_OUTPUT)
                        last_run_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
                        old_data = old_json

                    today_date = datetime.now().strftime('%Y-%m-%d')
                    is_today = (last_run_date == today_date)

                    for item in old_data:
                        t = item.get('ticker') or item.get('代號')
                        days = item.get('consecutive_days', 1)
                        if t:
                            consecutive_map[t] = days
        except Exception as e:
            print(f"[系統] 讀取舊資料失敗 (首次執行或檔案損壞): {e}")

        for item in final_results:
            t = item.get('ticker')
            if is_today:
                item['consecutive_days'] = consecutive_map.get(t, 1)
            else:
                item['consecutive_days'] = consecutive_map.get(t, 0) + 1
        
        # 儲存結果 (新結構)
        today_str = datetime.now().strftime('%Y-%m-%d')
        output_dict = {
            "last_run": today_str,
            "data": final_results
        }
        try:
            with open(MOMENTUM_OUTPUT, 'w', encoding='utf-8') as f:
                json.dump(output_dict, f, ensure_ascii=False, indent=4)
            print(f"[系統] 成功將結果儲存至 {MOMENTUM_OUTPUT}")
        except Exception as e:
            print(f"[錯誤] 儲存 JSON 失敗: {e}")

        df_final = pd.DataFrame(final_results)
        # 整理欄位名稱以符合用戶要求
        df_final.rename(columns={
            'ticker': '代號',
            'name': '名稱',
            'price': '現價',
            'volume': '今日成交量',
            'avg_vol_5d': '5日均量',
            'yoy': '營收YoY%',
            'consecutive_days': '連續進榜(天)'
        }, inplace=True)
        
        # 重新排序顯示欄位 (去掉輔助欄位 stock_id)
        display_cols = ['代號', '名稱', '現價', '今日成交量', '5日均量', '營收YoY%', '投信近3日買賣超(張)', '連續進榜(天)']
        print(df_final[display_cols].to_string(index=False))
        print("\n" + "★"*58)
        print(f"篩選結算：符合『技術 + 基本 + 籌碼』三面向之終極標的，共 {len(df_final)} 檔。")
    else:
        print("目前無標的同時符合『技術面、基本面與投信買超』之終極條件。")
    print("★"*58)
