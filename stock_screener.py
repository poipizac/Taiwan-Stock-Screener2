import yfinance as yf
import pandas as pd

def ten_bagger_screener(stock_list):
    print("啟動【10倍股】技術面掃描...")
    print("過濾條件：1. 站上 200MA | 2. 創 120 日新高 | 3. 均線糾結 (乖離 < 1.2倍)")
    
    results = []
    
    for ticker_symbol in stock_list:
        try:
            stock = yf.Ticker(ticker_symbol)
            # 抓取過去一年的日線數據 (確保有足夠天數計算 200MA 與 120日高點)
            hist = stock.history(period="1y")
            
            # 若上市時間過短，資料不足 200 天則跳過
            if len(hist) < 200:
                continue
                
            close_prices = hist['Close']
            high_prices = hist['High']
            
            # 1. 計算均線與現價
            current_price = close_prices.iloc[-1]
            ma10 = close_prices.rolling(window=10).mean().iloc[-1]
            ma200 = close_prices.rolling(window=200).mean().iloc[-1]
            
            # 2. 計算過去 120 日的最高價 (取前一天為基準，看今日是否突破)
            # shift(1) 代表不包含今天的過去 120 天最高點
            high_120 = high_prices.rolling(window=120).max().shift(1).iloc[-1]
            
            # 3. 計算均線乖離倍數 (10MA 與 200MA 的差距)
            # 圖片提及出場條件為 4 倍，因此進場的「均線糾結」濾網應設定在較低數值 (此處預設上限為 1.2 倍)
            deviation_ratio = ma10 / ma200 if ma200 > 0 else 0
            
            # --- 核心邏輯判斷 ---
            cond1_above_ma200 = current_price > ma200
            cond2_breakout_120 = current_price >= high_120
            cond3_entanglement = deviation_ratio < 1.2  # 確保在盤整起漲區，避免追高
            
            if cond1_above_ma200 and cond2_breakout_120 and cond3_entanglement:
                results.append({
                    "代號": ticker_symbol,
                    "現價": round(current_price, 2),
                    "200MA": round(ma200, 2),
                    "120日高點": round(high_120, 2),
                    "乖離倍數": round(deviation_ratio, 2)
                })
                
        except Exception as e:
            # 遇到下市或無資料的標的，安全跳過
            continue

    # 將結果轉換為 DataFrame 並回傳
    if not results:
        return pd.DataFrame(columns=["代號", "現價", "200MA", "120日高點", "乖離倍數"])
        
    return pd.DataFrame(results)

# 測試名單 (加入了圖中實證獲利的範例：5314 世紀，需加上 .TWO 代表櫃買中心)
# 你可以隨時替換成全台股的代碼列表
target_stocks = ['2330.TW', '2317.TW', '2454.TW', '3231.TW', '2382.TW', '5314.TWO'] 

df = ten_bagger_screener(target_stocks)
print("\n--- 🎯 十倍股策略篩選結果 ---")
if df.empty:
    print("目前測試名單中，無符合起漲突破型態的標的。")
else:
    print(df.to_string(index=False))