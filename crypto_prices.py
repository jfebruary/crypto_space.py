import json
import websocket
import pandas as pd
import time

# Lista kryptowalut do śledzenia
assets = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

# Konstruowanie streamów dla każdej kryptowaluty
streams = [coin.lower() + "@kline_1m" for coin in assets]

# Tworzenie URL do połączenia WebSocket z Binance
socket = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

# Inicjalizacja DataFrame
df = pd.DataFrame(columns=['symbol', 'price'])

# Funkcja obsługująca dane z WebSocket
def on_message(ws, message):
    global df
    data = json.loads(message)
    
    if 'data' in data and 'k' in data['data']:  # Sprawdzamy, czy wiadomość zawiera dane świecy
        kline = data['data']['k']
        symbol = kline['s']
        close_price = float(kline['c'])
        
        # Zaokrąglenie ceny do 1 miejsca po przecinku
        close_price = round(close_price, 1)
        
        # Tworzenie nowego wiersza jako DataFrame
        new_row = pd.DataFrame({'symbol': [symbol], 'price': [close_price]})
        
        # Sprawdzanie, czy new_row nie jest pusty przed konkatenacją
        if not new_row.isna().all().all() and not new_row.empty:
            df = pd.concat([df, new_row], ignore_index=True)

# Funkcja do połączenia z WebSocket
def on_open(ws):
    print("WebSocket connection opened")

# Funkcja do zamykania połączenia
def on_close(ws):
    print("WebSocket connection closed")
    # Zapisz dane do pliku CSV po zamknięciu połączenia
    df.to_csv('crypto_prices.csv', index=False)

# Funkcja obsługująca błędy
def on_error(ws, error):
    print(f"Error occurred: {error}")

# Tworzenie połączenia WebSocket
ws = websocket.WebSocketApp(socket, on_message=on_message, on_open=on_open, on_close=on_close, on_error=on_error)

# Funkcja do wyświetlania danych co 1 minutę
def refresh_display():
    while True:
        time.sleep(60)  # Czekaj 1 minutę
        if not df.empty:
            print("\n--- Aktualizacja ---")
            print(df.tail())  # Wyświetl ostatnie dane

# Uruchom WebSocket w osobnym wątku
import threading
wst = threading.Thread(target=ws.run_forever)
wst.start()

# Uruchom funkcję odświeżania wyświetlania co 1 minutę
refresh_display()