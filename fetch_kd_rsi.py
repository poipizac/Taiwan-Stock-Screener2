import json
import math
import os
import time
import warnings
import numpy as np
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

    denom = (high_max - low_min).replace(0, np.nan)
    rsv = ((df_weekly["Close"] - low_min) / denom) * 100.0
    rsv = rsv.fillna(50.0)

    # 台股標準 KD 遞迴計算 (平滑因子 1/3，初始值 50)
    k_vals = []
    d_vals = []
    k_prev, d_prev = 50.0, 50.0

    for val in rsv.iloc[n - 1:]:
        val_f = 50.0 if (pd.isna(val) or math.isnan(float(val))) else float(val)
        k_curr = (2.0 / 3.0) * k_prev + (1.0 / 3.0) * val_f
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

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(100.0)


def screen_kd_rsi():
    """
    主篩選流程：
    1. 下載過去一年 (period="1y") 日線資料
    2. 周線 KD(9,3,3) 最新一周黃金交叉 (前一周 K <= D 且 最新一周 K > D)
    3. 日線 RSI(14) > 50
    """
    results = []
    fetched_count = 0
    valid_kd_count = 0

    try:
        tickers = load_taiwan_tickers()
        if not tickers:
            print("[警告] 無可用股票名單，輸出空陣列。")
            return []

        codes_dict = twstock.codes
        batch_size = 50
        total_batches = (len(tickers) + batch_size - 1) // batch_size

        print(f"[啟動] 開始掃描「周KD金叉 + 日RSI強勢」標的 (清單總數: {len(tickers)} 檔，分 {total_batches} 批次，區間: period='1y')...")

        for i in tqdm(range(0, len(tickers), batch_size), desc="批次掃描進度"):
            batch = tickers[i:i + batch_size]
            try:
                # 抓取過去 1 年 (period="1y") 日線資料，確保約 52 周數據讓周 KD 充分平滑收斂
                data = yf.download(
                    batch,
                    period="1y",
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
                        if df.empty:
                            continue

                        # 統計：成功抓到日線資料的股票數
                        fetched_count += 1

                        # 1. 轉換為周線並計算周 KD(9,3,3)
                        df_weekly = calculate_weekly_kd(df, n=9)
                        if len(df_weekly) < 2:
                            continue

                        prev_k = df_weekly["K"].iloc[-2]
                        prev_d = df_weekly["D"].iloc[-2]
                        curr_k = df_weekly["K"].iloc[-1]
                        curr_d = df_weekly["D"].iloc[-1]

                        if pd.isna(prev_k) or pd.isna(prev_d) or pd.isna(curr_k) or pd.isna(curr_d):
                            continue

                        prev_k, prev_d = float(prev_k), float(prev_d)
                        curr_k, curr_d = float(curr_k), float(curr_d)

                        # 統計：周 KD 值有效 (非 NaN) 的股票數
                        valid_kd_count += 1

                        # 2. 計算日線 RSI(14)
                        rsi_series = calculate_daily_rsi(df["Close"], period=14)
                        if rsi_series.empty or pd.isna(rsi_series.iloc[-1]):
                            continue
                        latest_rsi = float(rsi_series.iloc[-1])

                        # 3. 篩選條件：周 KD 黃金交叉 (前一周 K <= D 且 最新一周 K > D) 且 日 RSI > 50
                        if (prev_k <= prev_d and curr_k > curr_d) and (latest_rsi > 50.0):
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

    # Debug 統計資訊輸出
    print("\n" + "=" * 55)
    print("📊 [Debug 統計報告 - 周KD金叉 + 日RSI強勢]")
    print(f"1. 總共成功抓取資料之股票檔數          : {fetched_count} 檔")
    print(f"2. 轉換周線後 KD 值有效 (非 NaN) 之檔數: {valid_kd_count} 檔")
    print(f"3. 最終符合『周KD金叉 + 日RSI>50』檔數 : {len(results)} 檔")
    print("=" * 55)

    return results


if __name__ == "__main__":
    final_list = []
    try:
        final_list = screen_kd_rsi()
    except Exception as e:
        print(f"[嚴重錯誤] 執行失敗，將輸出空陣列: {e}")
        final_list = []

    # 確保無論有無結果或發生錯誤，皆輸出合法 JSON 陣列
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(final_list if isinstance(final_list, list) else [], f, ensure_ascii=False, indent=4)
        print(f"[完成] 結果已匯出至 {OUTPUT_FILE} (共 {len(final_list)} 檔)")
    except Exception as io_err:
        print(f"[錯誤] 寫入 {OUTPUT_FILE} 失敗: {io_err}")
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("[]")
