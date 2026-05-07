from FinMind.data import DataLoader
import pandas as pd
from datetime import datetime

def test_finmind():
    dl = DataLoader()
    # FinMind might have this data
    # Let's try to see available datasets or common ones
    try:
        # 嘗試抓取今日處置股 (如果 FinMind 有提供)
        # 參考常用名稱: TaiwanStockDisposition
        df = dl.taiwan_stock_disposition(
            start_date=datetime.now().strftime('%Y-%m-%d')
        )
        print(df.head())
    except Exception as e:
        print(f"FinMind error: {e}")

if __name__ == "__main__":
    test_finmind()
