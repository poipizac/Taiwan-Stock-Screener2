import json
import os
import time
import warnings
from datetime import datetime
import pandas as pd
import yfinance as yf
import twstock
from tqdm import tqdm

warnings.filterwarnings("ignore")

OUTPUT_FILE = "./kd_rsi_data.json"


def load_taiwan_tickers():
    """
    從 twstock 取得全市場上市與上櫃 4 碼普通股名單。
    """
    print("[系統] 正在從 twstock 載入全市場上市櫃股票名單...")
    tickers = []
    for code, info in twstock.codes.items():
        if info.type == "股票" and info.market in ["上市", "上櫃"] and len(code) == 4 and code.isdigit():
            suffix = ".TW" if info.market == "上市" else ".TWO"
            tickers.append(f"{code}{suffix}")
    return sorted(tickers)


def calculate_weekly_kd(df_daily, n=9):
    """
    將日線資料重採樣為周線 (W-FRI)，計算周 KD (參數 9, 3, 3)。
    回傳重採樣後的周線 DataFrame (含 K, D 欄位)。
    """
    df_weekly = df_daily.resample("W-FRI").agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum"
    }).dropna(subset=["High", "Low", "Close"])

    if len(df_weekly) < n + 2:
        return pd.DataFrame()

    low_min = df_weekly["Low"].rolling(window=n).min()
    high_max = df_weekly["High"].rolling(window=n).max()

    denom = high_max - low_min
    rsv = ((df_weekly["Close"] - low_min) / denom.replace(0, pd.NA)) * 100.0
    rsv = rsv.fillna(50.0)

    # 台股標準 KD 遞迴計算 (平滑因子 1/3，初始值 50)
    k_vals = []
    d_vals = []
    k_prev, d_prev = 50.0, 50.0

    for val in rsv.iloc[n - 1:]:
        k_curr = (2.0 / 3.0) * k_prev + (1.0 / 3.0) * float(val)
        d_curr = (2.0 / 3.0) * d_prev + (1.0 / 3.0) * k_curr
        k_vals.append(k_curr)
        d_vals.append(d_curr)
        k_prev, d_prev = k_curr, d_curr

    df_weekly = df_weekly.iloc[n - 1:].copy()
    df_weekly["K"] = k_vals
    df_weekly["D"] = d_vals
    return df_weekly


def calculate_daily_rsi(close_series, period=14):
    """
    計算日線 RSI (參數 14)，採用 Wilder's Smoothing (alpha = 1/period)。
    """
    if len(close_series) < period + 1:
        return pd.Series(dtype=float)

    delta = close_series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(100.0)


def screen_kd_rsi():
    """
    主篩選流程：
    1. 周線 KD(9,3,3) 最新一周黃金交叉 (前一周 K <= D 且 最新一周 K > D)
    2. 日線 RSI(14) > 50
    """
    results = []
    try:
        tickers = load_taiwan_tickers()
        if not tickers:
            print("[警告] 無可用股票名單，輸出空陣列。")
            return []

        codes_dict = twstock.codes
        batch_size = 50
        total_batches = (len(tickers) + batch_size - 1) // batch_size

        print(f"[啟動] 開始掃描「周KD金叉 + 日RSI強勢」標的 (共 {len(tickers)} 檔，分 {total_batches} 批次)...")

        for i in tqdm(range(0, len(tickers), batch_size), desc="批次掃描進度"):
            batch = tickers[i:i + batch_size]
            try:
                data = yf.download(
                    batch,
                    period="6mo",
                    interval="1d",
                    group_by="ticker",
                    threads=True,
                    progress=False
                )

                if data.empty:
                    continue

                for ticker in batch:
                    try:
                        if isinstance(data.columns, pd.MultiIndex):
                            if ticker not in data.columns.levels[0]:
                                continue
                            df = data[ticker].copy()
                        else:
                            df = data.copy()

                        df = df.dropna(subset=["High", "Low", "Close"])
                        if len(df) < 60:
                            continue

                        # 1. 計算日線 RSI(14) 並檢查是否 > 50
                        rsi_series = calculate_daily_rsi(df["Close"], period=14)
                        if rsi_series.empty or pd.isna(rsi_series.iloc[-1]):
                            continue
                        latest_rsi = float(rsi_series.iloc[-1])
                        if latest_rsi <= 50.0:
                            continue

                        # 2. 計算周線 KD(9,3,3) 並檢查最新一周是否黃金交叉
                        df_weekly = calculate_weekly_kd(df, n=9)
                        if len(df_weekly) < 2:
                            continue

                        prev_k = float(df_weekly["K"].iloc[-2])
                        prev_d = float(df_weekly["D"].iloc[-2])
                        curr_k = float(df_weekly["K"].iloc[-1])
                        curr_d = float(df_weekly["D"].iloc[-1])

                        # 黃金交叉：前一周 K <= D 且 最新一周 K > D
                        if prev_k <= prev_d and curr_k > curr_d:
                            stock_id = ticker.split(".")[0]
                            tw_info = codes_dict.get(stock_id)
                            stock_name = tw_info.name if tw_info else ticker
                            latest_close = float(df["Close"].iloc[-1])

                            results.append({
                                "ticker": ticker,
                                "stock_id": stock_id,
                                "name": stock_name,
                                "close": round(latest_close, 2),
                                "weekly_k": round(curr_k, 2),
                                "weekly_d": round(curr_d, 2),
                                "daily_rsi": round(latest_rsi, 2)
                            })
                    except Exception:
                        continue

                time.sleep(0.5)
            except Exception as batch_err:
                print(f"[警告] 批次下載發生異常: {batch_err}")
                continue

    except Exception as e:
        print(f"[錯誤] 篩選過程發生例外錯誤: {e}")
        return []

    return results


if __name__ == "__main__":
    final_list = []
    try:
        final_list = screen_kd_rsi()
    except Exception as e:
        print(f"[嚴重錯誤] 執行失敗，將輸出空陣列: {e}")
        final_list = []

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(final_list if isinstance(final_list, list) else [], f, ensure_ascii=False, indent=4)
        print(f"\n[完成] 篩選結束，共找到 {len(final_list)} 檔符合條件標的，已匯出至 {OUTPUT_FILE}")
    except Exception as io_err:
        print(f"[錯誤] 寫入 {OUTPUT_FILE} 失敗: {io_err}")
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("[]")
