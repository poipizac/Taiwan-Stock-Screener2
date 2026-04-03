
import re
import yfinance as yf
import pandas as pd

def extract_tickers(html_path):
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()
    tickers = re.findall(r'<code>(.*?)</code>', content)
    return tickers

def fetch_technical_data(tickers):
    data_results = []
    for ticker in tickers:
        try:
            print(f"Fetching {ticker}...")
            # Need at least 200 days of history for SMA200
            # Period 1y should be enough
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1y")
            if hist.empty:
                continue
                
            last_close = hist['Close'].iloc[-1]
            sma10 = hist['Close'].rolling(window=10).mean().iloc[-1]
            sma200 = hist['Close'].rolling(window=200).mean().iloc[-1]
            high120 = hist['High'].rolling(window=120).max().iloc[-1]
            
            data_results.append({
                "ticker": ticker,
                "close": last_close,
                "sma10": sma10,
                "sma200": sma200,
                "high120": high120
            })
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
    return data_results

if __name__ == "__main__":
    html_path = 'd:/Antigravity/Stock_finder/stock_screener_report.html'
    tickers = extract_tickers(html_path)
    print(f"Found {len(tickers)} tickers.")
    tech_data = fetch_technical_data(tickers)
    df = pd.DataFrame(tech_data)
    df.to_csv('d:/Antigravity/Stock_finder/temp_tech_data.csv', index=False)
    print("Saved to temp_tech_data.csv")
