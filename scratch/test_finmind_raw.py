from FinMind.data import DataLoader
import os
from datetime import datetime

def test_finmind_raw():
    token = os.environ.get('FINMIND_TOKEN', '')
    dl = DataLoader()
    if token:
        dl.login_by_token(token)
    
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        # 嘗試使用通用 get_data 方法
        df = dl.taiwan_stock_disposition(start_date=today)
        print("Success using dl.taiwan_stock_disposition")
        print(df.head())
    except Exception as e:
        print(f"Failed dl.taiwan_stock_disposition: {e}")
        try:
            # 某些版本可能 dataset 名稱不同
            df = dl.get_data(dataset='TaiwanStockDisposition', start_date=today)
            print("Success using get_data(dataset='TaiwanStockDisposition')")
            print(df.head())
        except Exception as e2:
            print(f"Failed get_data: {e2}")

if __name__ == "__main__":
    test_finmind_raw()
