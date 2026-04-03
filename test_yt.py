
import yfinance as yf
import twstock

codes = twstock.codes
tickers = []
for code, info in codes.items():
    if info.type == '股票' and info.market in ['上市', '上櫃']:
        suffix = '.TW' if info.market == '上市' else '.TWO'
        tickers.append(f"{code}{suffix}")

print(f"Sample tickers: {tickers[:5]}")

for t in tickers[:3]:
    print(f"Fetching {t}...")
    stock = yf.Ticker(t)
    hist = stock.history(period="1y")
    print(f"{t} hist length: {len(hist)}")
