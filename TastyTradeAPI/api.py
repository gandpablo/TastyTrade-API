import requests
import json
import websocket
import pandas as pd
from datetime import timedelta
import datetime
import math
import threading
import time


class TastyTradeAPI:

    # ------- ES: GENERAL -----------------
    # ------- EN: GENERAL -----------------

    def __init__(self):
        # FIX #1: Updated to current TastyTrade API domain (tastyworks.com is legacy)
        self._API_URL = 'https://api.tastytrade.com'

    def _get_auth_header(self, session_token: str) -> str:
        """
        FIX #2: Handle both legacy session tokens and OAuth2 Bearer tokens.
        OAuth2 tokens (JWT) start with 'ey' and need 'Bearer ' prefix.
        """
        if session_token.startswith('ey') and not session_token.startswith('Bearer '):
            return f"Bearer {session_token}"
        elif session_token.startswith('Bearer '):
            return session_token
        else:
            return session_token  # Legacy session token format

    def _get_token(self):
        """
        EN: Obtains the session token using user credentials.
        ES: Obtiene el token de sesión utilizando las credenciales del usuario.
        """
        url = f"{self._API_URL}/sessions"
        headers = {
            'Content-Type': 'application/json'
        }

        payload = {
            "login": self._USER,
            "password": self._PASS,
            "remember-me": True
        }

        response = requests.post(url, headers=headers, data=json.dumps(payload))

        if response.status_code in [200, 201]:
            data = response.json()
            session_token = data['data']['session-token']
            return session_token
        else:
            raise Exception(f"Error obtaining session token | Status Code: {response.status_code}")

    def _get_accounts(self, session_token: str) -> list:
        """
        EN: Obtains the list accounts associated with the client.
        ES: Obtiene la lista de cuentas asociadas al cliente.
        """
        url = f"{self._API_URL}/customers/me/accounts"

        headers = {
            "Authorization": self._get_auth_header(session_token)
        }

        response = requests.get(url, headers=headers)

        if response.status_code in [200, 201]:
            data = response.json()
            data = data['data']['items']
            return data
        else:
            raise Exception(f"Error retrieving account data | Status Code: {response.status_code}")

    def _account_number(self, session_token: str, num: int = 0) -> str:
        """
        EN: Obtains the account number based on the provided index, default is 0.
        ES: Obtiene el número de cuenta basado en el indice proporcionado, por defecto 0.
        """
        data = self._get_accounts(session_token)

        try:
            if len(data) == 0:
                ac_num = 'No Account'
            else:
                data = data[num]['account']
                ac_num = data['account-number']
            return ac_num

        except Exception as e:
            raise Exception(f"Error retrieving account number --->\n{e}")

    def _get_client_info(self, session_token: str) -> dict:
        """
        EN: Obtains general information about the client (no account).
        ES: Obtiene información general sobre el cliente (no de la cuenta).
        """
        url = f"{self._API_URL}/customers/me"

        headers = {
            "Authorization": self._get_auth_header(session_token)
        }

        response = requests.get(url, headers=headers)

        if response.status_code in [200, 201]:
            data = response.json()
            data = data['data']
            return data
        else:
            raise Exception(f"Error retrieving general data | Status Code: {response.status_code}")

    # -------------------   ES: INICIALIZAR CLIENTE   -------------------
    # -------------------   EN: INITIALIZE CLIENT   ---------------------

    def Client(self, USER: str, PASS: str, num: int = 0) -> dict:
        """
        EN: Creates a TastyTrade client with USER and PASS.
        ES: Crea un cliente de TastyTrade con USER y PASS.
        """
        self._USER = USER
        self._PASS = PASS

        session_token = self._get_token()
        ac_num = self._account_number(session_token, num)

        self._Client = {'session_token': session_token, 'account_number': ac_num}

        return {'session_token': session_token, 'account_number': ac_num}

    def _get__balances(self, Client: dict) -> dict:
        """
        EN: Obtains the general info of the account associated to the client (balances).
        ES: Obtiene la información general de la cuenta asociada al cliente (balances).
        """
        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client.")

        url = f"{self._API_URL}/accounts/{account_number}/balances"

        headers = {
            "Authorization": self._get_auth_header(session_token)
        }

        response = requests.get(url, headers=headers)

        if response.status_code in [200, 201]:
            data = response.json()
            data = data['data']
            return data
        else:
            raise Exception(f"Error retrieving balances | Status Code: {response.status_code}")

    # -------------------   ES: MÉTRICAS DE LA CUENTA   -------------------
    # -------------------   EN: ACCOUNT METRICS   -------------------

    def client_info(self, Client: dict) -> dict:
        """
        EN: Obtains basic information about the client.
        ES: Obtiene información básica sobre el cliente.
        """
        data = self._get_client_info(Client['session_token'])

        return {
            "name": f"{data.get('first-name', '')} {data.get('last-name', '')}".strip(),
            "email": data.get("email", ""),
            "citizenship_country": data.get("citizenship-country", ""),
            "liquid_net_worth": data.get("customer-suitability", {}).get("liquid-net-worth")
        }

    def balance(self, Client: dict) -> float:
        """
        EN: Obtains the cash balance of the account.
        ES: Obtiene el balance de efectivo de la cuenta.
        """
        data = self._get__balances(Client)
        try:
            return float(data['cash-balance'])
        except Exception as e:
            raise Exception(f"Error retrieving balances --->\n{e}")

    def equity_BP(self, Client: dict) -> float:
        """
        EN: Obtains the equity buying power.
        ES: Obtiene el equity buying power.
        """
        data = self._get__balances(Client)
        try:
            return float(data['equity-buying-power'])
        except Exception as e:
            raise Exception(f"Error retrieving Equity Buying Power --->\n{e}")

    def derivative_BP(self, Client: dict) -> float:
        """
        EN: Obtains the derivative buying power.
        ES: Obtiene el derivative buying power.
        """
        data = self._get__balances(Client)
        try:
            return float(data['derivative-buying-power'])
        except Exception as e:
            raise Exception(f"Error retrieving Derivative Buying Power --->\n{e}")

    def liquidity(self, Client: dict) -> float:
        """
        EN: Obtains the liquidity of the account.
        ES: Obtiene la liquidity de la cuenta.
        """
        data = self._get__balances(Client)
        try:
            return float(data['net-liquidating-value'])
        except Exception as e:
            raise Exception(f"Error retrieving Net Liquidating Value --->\n{e}")

    def total_fees(self, Client: dict) -> float:
        """
        EN: Obtains the total fees applied to the account.
        ES: Obtiene los fees totales aplicados a la cuenta.
        """
        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client.")

        url = f"{self._API_URL}/accounts/{account_number}/transactions/total-fees"

        headers = {
            "Authorization": self._get_auth_header(session_token)
        }

        response = requests.get(url, headers=headers)

        if response.status_code in [200, 201]:
            data = response.json()
            data = data['data']['total-fees']
            return data
        else:
            raise Exception(f"Error retrieving the applied fees | Status Code: {response.status_code}")

    # -------------------   ES: EJECUTAR ORDENES   -------------------
    # -------------------   EN: PLACE ORDERS   -------------------

    def _start_long(self, ticker: str, val: int, time_force: str, otype: str) -> str:
        order = {
            "time-in-force": time_force,
            "order-type": otype,
            "legs": [{
                "instrument-type": "Equity",
                "symbol": ticker,
                "quantity": val,
                "action": "Buy to Open"
            }]
        }
        return json.dumps(order)

    def _end_long(self, ticker: str, val: int, time_force: str, otype: str) -> str:
        order = {
            "time-in-force": time_force,
            "order-type": otype,
            "legs": [{
                "instrument-type": "Equity",
                "symbol": ticker,
                "quantity": val,
                "action": "Sell to Close"
            }]
        }
        return json.dumps(order)

    def _start_short(self, ticker: str, val: int, time_force: str, otype: str) -> str:
        order = {
            "time-in-force": time_force,
            "order-type": otype,
            "legs": [{
                "instrument-type": "Equity",
                "symbol": ticker,
                "quantity": val,
                "action": "Sell to Open"
            }]
        }
        return json.dumps(order)

    def _end_short(self, ticker: str, val: int, time_force: str, otype: str) -> str:
        order = {
            "time-in-force": time_force,
            "order-type": otype,
            "legs": [{
                "instrument-type": "Equity",
                "symbol": ticker,
                "quantity": val,
                "action": "Buy to Close"
            }]
        }
        return json.dumps(order)

    def order(self, Client: dict, ticker: str, val: int, order_type: str, action: str,
              time_force: str = 'Day', otype: str = 'Market') -> dict:
        """
        EN: Places a market order for the specified ticker and quantity.
        ES: Realiza una orden de mercado para el ticker y cantidad especificados.
        """
        if order_type == 'Long':
            if action == 'Start':
                order = self._start_long(ticker, val, time_force, otype)
            elif action == 'End':
                order = self._end_long(ticker, val, time_force, otype)
            else:
                raise Exception("Order error ---> Invalid action, try: Start or End")
        elif order_type == 'Short':
            if action == 'Start':
                order = self._start_short(ticker, val, time_force, otype)
            elif action == 'End':
                order = self._end_short(ticker, val, time_force, otype)
            else:
                raise Exception("Order error ---> Invalid action, try: Start or End")
        else:
            raise Exception("Order error ---> Invalid order type, try: Long or Short")

        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client.")

        url = f"{self._API_URL}/accounts/{account_number}/orders"

        headers = {
            "Authorization": self._get_auth_header(session_token),
            'Content-Type': 'application/json'
        }

        response = requests.post(url, headers=headers, data=order)

        if response.status_code not in [200, 201]:
            raise Exception(f"Order error ---> Response Status: {response.status_code}")

        data = response.json()
        ord_data = data["data"]["order"]
        fees = data["data"]["fee-calculation"]
        bp = data["data"]["buying-power-effect"]

        return {
            "Status": ord_data["status"],
            "Size": ord_data["size"],
            "Fees": float(fees["total-fees"]),
            "Commission": float(fees["commission"]),
            "NewBuyingPower": float(bp["new-buying-power"]),
            "ReceivedAt": ord_data["received-at"]
        }

    # ------------------- ES: DATOS DE LAS POSICIONES -------------------
    # ------------------- EN: POSITION DATA -------------------

    def _position(self, Client: dict) -> list:
        """
        EN: Obtains the list of open positions.
        ES: Obtiene la lista de las posiciones abiertas.
        """
        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client.")

        url = f"{self._API_URL}/accounts/{account_number}/positions"

        headers = {
            "Authorization": self._get_auth_header(session_token)
        }

        response = requests.get(url, headers=headers)

        if response.status_code in [200, 201]:
            data = response.json()
            data = data['data']['items']
            return data
        else:
            raise Exception(f"Error retrieving positions | Status Code: {response.status_code}")

    def _parse_position_data(self, data: dict) -> dict:
        """Parses position data to return a user-friendly dictionary."""

        def format_date(date_str: str) -> str:
            try:
                dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                return date_str

        symbol = data.get("symbol")
        quantity = float(data.get("quantity", 0))
        direction = data.get("quantity-direction")
        avg_entry_price = float(data.get("average-open-price", 0))
        current_price = float(data.get("close-price", 0))
        multiplier = float(data.get("multiplier", 1))

        if direction and direction.lower() == "long":
            profit = round((current_price - avg_entry_price) * quantity * multiplier, 3)
        elif direction and direction.lower() == "short":
            profit = round((avg_entry_price - current_price) * quantity * multiplier, 3)
        else:
            profit = None

        return {
            "symbol": symbol,
            "quantity": quantity,
            "type": direction,
            "profit": profit,
            "avg_entry_price": avg_entry_price,
            "current_price": current_price,
            "opened_at": format_date(data.get("created-at", "")),
            "updated_at": format_date(data.get("updated-at", "")),
        }

    def all_positions(self, Client: dict) -> dict:
        """
        EN: Obtains all open positions.
        ES: Obtiene todas las posiciones abiertas.
        """
        try:
            positions = self._position(Client)

            if len(positions) == 0:
                return {}

            pos = {}
            for p in positions:
                try:
                    parsed = self._parse_position_data(p)
                    pos[parsed['symbol']] = parsed
                except Exception as e:
                    symbol = p.get('symbol', 'unknown')
                    pos[symbol] = {"error": str(e)}

            return pos

        except Exception as e:
            raise Exception(f"Error parsing positions --->\n{e}")

    def check_position(self, Client: dict, symbol: str) -> dict:
        """
        EN: Checks if there is an open position for the given symbol.
        ES: Comprueba si hay una posición abierta para el símbolo dado.
        """
        positions = self.all_positions(Client)
        return positions.get(symbol, None)

    # ------------------- ES: DATOS DE LAS TRANSACCIONES -------------------
    # ------------------- EN: TRANSACTION DATA -------------------

    def _transaction(self, Client: dict) -> list:
        """
        EN: Obtains the list of transactions.
        ES: Obtiene la lista de transacciones.
        """
        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client.")

        # FIX #3: Added missing slash before 'transactions'
        url = f"{self._API_URL}/accounts/{account_number}/transactions"

        headers = {
            "Authorization": self._get_auth_header(session_token)
        }

        response = requests.get(url, headers=headers)

        if response.status_code in [200, 201]:
            data = response.json()
            data = data['data']['items']
            return data
        else:
            raise Exception(f"Error retrieving transactions | Status Code: {response.status_code}")

    def _parse_transaction(self, item: dict) -> dict:
        """Parses transaction data to return a user-friendly dictionary."""

        def format_date(date_str: str) -> str:
            try:
                dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                return date_str

        return {
            "id": item.get("id"),
            "transaction_type": item.get("transaction-sub-type") or item.get("transaction-type"),
            "description": item.get("description"),
            "quantity": float(item["quantity"]) if item.get("quantity") else None,
            "price": float(item["price"]) if item.get("price") else None,
            "value": float(item["value"]) if item.get("value") else None,
            "date": format_date(item.get("executed-at", "")),
        }

    def all_transactions(self, Client: dict) -> dict:
        """
        EN: Obtains all transactions.
        ES: Obtiene todas las transacciones.
        """
        transactions = self._transaction(Client)
        data = {}

        if len(transactions) == 0:
            return data

        for t in transactions:
            try:
                symbol = t.get("symbol")
                parsed = self._parse_transaction(t)
                if symbol not in data:
                    data[symbol] = []
                data[symbol].append(parsed)
            except Exception as e:
                symbol = t.get("symbol", "unknown")
                if symbol not in data:
                    data[symbol] = []
                data[symbol].append({"error": str(e)})

        return data

    def check_transaction(self, Client: dict, symbol: str) -> dict:
        """
        EN: Checks if there are transactions for the given symbol.
        ES: Comprueba si hay transacciones para el símbolo dado.
        """
        transactions = self.all_transactions(Client)
        return transactions.get(symbol, None)

    def still_connected(self, Client):
        """Check if the client session is still valid."""
        session_token = Client['session_token']
        try:
            _ = self._get_client_info(session_token)
            return True
        except:
            return False

    # -------------------  MARKET DATA WITH DX FEED ---------------

    def _get_DX_vals(self, Client: dict) -> dict:
        """
        EN: Obtains the token to authorize in the DX feed API.
        ES: Obtienes el token para autorizarte en la API de feed DX.
        """
        session_token = Client['session_token']

        url = f"{self._API_URL}/api-quote-tokens"

        headers = {
            "Authorization": self._get_auth_header(session_token)
        }

        response = requests.get(url, headers=headers)

        if response.status_code in [200, 201]:
            data = response.json()
            return data['data']
        else:
            raise Exception(f"Error retrieving DX session | Status Code: {response.status_code}")

    def DX_link(self, Client: dict) -> str:
        """
        EN: Obtains the url api to DX feed.
        ES: Obtienes la url de la api para DX feed.
        """
        data = self._get_DX_vals(Client)
        try:
            return data['dxlink-url']
        except Exception as e:
            raise Exception(f"Error retrieving DX LINK --->\n{e}")

    def DX_token(self, Client: dict) -> str:
        """
        EN: Returns the DX token.
        ES: Devuelve el token de DX.
        """
        data = self._get_DX_vals(Client)
        try:
            return data['token']
        except Exception as e:
            raise Exception(f"Error retrieving DX TOKEN --->\n{e}")

    def DX_messages(self, dx_token: str, ticker_list: list) -> dict:
        """
        EN: Creates messages to communicate with DX websocket for real-time data.
        ES: Crea los mensajes para comunicarse con el websocket de DX para datos en tiempo real.
        """
        if len(ticker_list) == 0:
            return None

        lista = []
        for i in ticker_list:
            val = {"type": "Quote", "symbol": i}
            lista.append(val)

        # FIX #8: Use COMPACT format (FULL is being deprecated per TastyTrade docs)
        messages = {
            'SETUP': {"type": "SETUP", "channel": 0, "version": "0.1-DXF-JS/0.3.0",
                      "keepaliveTimeout": 60, "acceptKeepaliveTimeout": 60},
            'AUTH': {"type": "AUTH", "token": dx_token, "channel": 0},
            'CHANNEL': {"type": "CHANNEL_REQUEST", "channel": 3, "service": "FEED",
                        "parameters": {"contract": "AUTO"}},
            'FEED': {"type": "FEED_SETUP", "channel": 3, "acceptAggregationPeriod": 0.1,
                     "acceptDataFormat": "COMPACT",
                     "acceptEventFields": {"Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice"]}},
            'SUB': {"type": "FEED_SUBSCRIPTION", "channel": 3, "reset": True, "add": lista},
            'KEEP': {"type": "KEEPALIVE", "channel": 0}
        }

        return messages

    def _get_hist_DX_messages(self, dx_token: str, ticker_list: list, num: int, t_time: str,
                              fromTime, candle_fields: list = None) -> dict:
        """
        EN: Creates messages for DX websocket to retrieve historical Candle data.
        ES: Crea los mensajes para el websocket de DX para obtener datos históricos de Candle.

        Based on TastyTrade DXLink documentation:
        - Symbol format: AAPL{=5m} where 5=period, m=type (minutes)
        - Must use COMPACT data format
        - fromTime is Unix epoch milliseconds
        """
        if len(ticker_list) == 0:
            return None

        # FIX #9: Default Candle fields for COMPACT format (order matters!)
        if candle_fields is None:
            candle_fields = ['eventType', 'eventSymbol', 'time', 'open', 'high', 'low', 'close', 'volume']

        # Build subscription list with Candle symbols
        lista = []
        for ticker in ticker_list:
            candle_symbol = f"{ticker}{{={num}{t_time}}}"
            subscription = {
                "type": "Candle",
                "symbol": candle_symbol,
                "fromTime": fromTime
            }
            lista.append(subscription)

        # FIX #8: Use COMPACT format with proper keepalive
        messages = {
            'SETUP': {"type": "SETUP", "channel": 0, "version": "0.1-DXF-JS/0.3.0",
                      "keepaliveTimeout": 60, "acceptKeepaliveTimeout": 60},
            'AUTH': {"type": "AUTH", "channel": 0, "token": dx_token},
            'CHANNEL': {"type": "CHANNEL_REQUEST", "channel": 3, "service": "FEED",
                        "parameters": {"contract": "AUTO"}},
            'FEED': {"type": "FEED_SETUP", "channel": 3, "acceptAggregationPeriod": 0.1,
                     "acceptDataFormat": "COMPACT",
                     "acceptEventFields": {"Candle": candle_fields}},
            'SUB': {"type": "FEED_SUBSCRIPTION", "channel": 3, "reset": True, "add": lista},
            'KEEP': {"type": "KEEPALIVE", "channel": 0}
        }

        return messages

    # -------------------   WEBSOCKET FOR HISTORICAL DATA   ---------------------

    def _websocket_historical(self, link: str, messages: dict, df: pd.DataFrame,
                              num_vars: int, timeout: int = 30) -> pd.DataFrame:
        """
        EN: Connects to DX websocket and retrieves historical Candle data.
        ES: Se conecta al websocket de DX y recupera datos históricos de Candle.

        FIX #6: Added proper timeout and completion detection so the WebSocket
        actually closes instead of hanging forever.
        """
        rows_collected = []
        data_received = threading.Event()
        connection_ready = threading.Event()
        ws_instance = [None]

        def add_rows_to_list(data_array):
            """Parse incoming COMPACT format data array."""
            if not data_array or not isinstance(data_array, list):
                return

            for i in range(0, len(data_array), num_vars):
                if i + num_vars <= len(data_array):
                    row = data_array[i:i + num_vars]
                    rows_collected.append(row)

            data_received.set()

        def treat_message(ws, message):
            try:
                message_data = json.loads(message)
                msg_type = message_data.get("type")

                if msg_type == "AUTH_STATE":
                    state = message_data.get("state")
                    if state == 'UNAUTHORIZED':
                        ws.send(json.dumps(messages['AUTH']))
                    elif state == 'AUTHORIZED':
                        ws.send(json.dumps(messages['CHANNEL']))

                elif msg_type == "CHANNEL_OPENED" and message_data.get("channel") == 3:
                    ws.send(json.dumps(messages['FEED']))

                elif msg_type == "FEED_CONFIG" and message_data.get("channel") == 3:
                    ws.send(json.dumps(messages['SUB']))
                    connection_ready.set()

                elif msg_type == "FEED_DATA" and message_data.get("channel") == 3:
                    data = message_data.get('data', [])
                    # COMPACT format: ["Candle", [data_values...]]
                    if len(data) >= 2 and isinstance(data[1], list):
                        add_rows_to_list(data[1])

            except json.JSONDecodeError:
                pass
            except Exception as e:
                print(f"Error processing message: {e}")

        def on_message(ws, message):
            treat_message(ws, message)

        def on_error(ws, error):
            print(f'WebSocket Error: {error}')
            data_received.set()

        def on_close(ws, close_status_code=None, close_msg=None):
            data_received.set()

        def on_open(ws):
            ws.send(json.dumps(messages['SETUP']))

        ws = websocket.WebSocketApp(link,
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws_instance[0] = ws

        # FIX #6: Run WebSocket in a daemon thread so we can implement timeout
        ws_thread = threading.Thread(target=ws.run_forever, daemon=True)
        ws_thread.start()

        # Wait for connection to be ready
        connection_ready.wait(timeout=10)

        # Wait for data with timeout
        data_received.wait(timeout=timeout)

        # Brief pause to allow final messages
        time.sleep(2)

        # Close the WebSocket connection
        if ws_instance[0]:
            ws_instance[0].close()

        ws_thread.join(timeout=5)

        # Batch-add all rows to DataFrame
        if rows_collected and len(df.columns) > 0:
            for row in rows_collected:
                if len(row) == len(df.columns):
                    df.loc[len(df)] = row

        return df

    # -------------------   HISTORICAL DATA   -------------------

    def get_historical(self, Client: dict, tickers: list, interval: str,
                       vars: list = None, max_data: int = 100) -> dict:
        """
        EN: Obtains historical Candle data for specified tickers and interval.
        ES: Obtiene datos históricos de Candle para los tickers e intervalo especificados.

        Args:
            Client: Authentication dict with session_token and account_number
            tickers: List of ticker symbols (e.g., ["AAPL", "MSFT"])
            interval: Time interval string (e.g., "1m", "5m", "15m", "1h", "1d")
            vars: List of OHLCV fields (e.g., ['open', 'high', 'low', 'close', 'volume'])
            max_data: Maximum number of data points per ticker

        Returns:
            Dict mapping ticker symbols to DataFrames with historical data
        """
        link = self.DX_link(Client)
        token = self.DX_token(Client)

        # FIX #4: Properly parse interval string (e.g., "15m" -> num=15, t_time="m")
        num_str = ''.join([c for c in interval if c.isdigit()])
        t_time = ''.join([c for c in interval if c.isalpha()])
        num = int(num_str) if num_str else 1

        if len(tickers) == 0:
            return {}

        # FIX #5: Don't mutate default argument - create a new list
        # Build complete list of Candle fields for DXLink
        user_fields = list(vars) if vars else ['close']

        # DXLink Candle fields (order matters for COMPACT format!)
        candle_fields = ['eventType', 'eventSymbol', 'time']
        for field in ['open', 'high', 'low', 'close', 'volume']:
            if field in user_fields:
                candle_fields.append(field)

        # Calculate how far back to fetch data
        days = int((max_data * num) / 6)
        if t_time == 'd':
            days = max_data * num
        elif t_time == 'h':
            days = int((max_data * num) / 6)

        # Timeout based on data volume
        wait_time = 30
        if days > 30 or len(tickers) > 5:
            wait_time = 45

        max_rows = int(max_data)

        # Calculate fromTime in milliseconds
        now = datetime.datetime.now(datetime.UTC)
        fromTime = int((now - timedelta(days=max(days, 1))).timestamp() * 1000)

        # Build DXLink messages
        messages = self._get_hist_DX_messages(token, tickers, num, t_time, fromTime, candle_fields)

        # Column name mapping (DXLink field -> DataFrame column)
        vars_names = {
            'eventType': 'EventType',
            'eventSymbol': 'Ticker',
            'time': 'Time',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }

        cols = [vars_names[v] for v in candle_fields if v in vars_names]
        df = pd.DataFrame(columns=cols)

        # Fetch data via WebSocket
        df_new = self._websocket_historical(link, messages, df, num_vars=len(candle_fields), timeout=wait_time)

        # FIX #7: Check for empty DataFrame before processing
        if df_new.empty:
            return {}

        if 'Ticker' not in df_new.columns or 'Time' not in df_new.columns:
            return {}

        def treat_tck(tck):
            """Remove candle interval suffix from ticker symbol."""
            tck = str(tck)
            cad = tck.split('{')[0]
            return cad

        def dividir_por_empresa(df, max_rows):
            """Split DataFrame by ticker symbol into dictionary of DataFrames."""
            diccionario_empresas = {}
            for empresa in df['Ticker'].unique():
                df_empresa = df[df['Ticker'] == empresa].copy()
                df_empresa = df_empresa.drop(columns=['Ticker'])
                df_empresa = df_empresa.head(max_rows)
                diccionario_empresas[empresa] = df_empresa
            return diccionario_empresas

        df_new['Ticker'] = df_new['Ticker'].apply(treat_tck)
        df_new['Time'] = df_new['Time'] / 1000
        df_new['Date'] = pd.to_datetime(df_new['Time'], unit='s')
        df_new = df_new.drop(columns=['Time'])

        # Filter for regular trading hours (9:30 AM - 4:00 PM ET, shown in UTC as ~14:30-21:00)
        df_new = df_new[
            ((df_new['Date'].dt.hour > 14) | ((df_new['Date'].dt.hour == 14) & (df_new['Date'].dt.minute >= 30))) &
            (df_new['Date'].dt.hour < 21)
        ]

        dicc = dividir_por_empresa(df_new, max_rows)

        return dicc

    def values_from_data(self, interval: str, date) -> int:
        """
        EN: Calculates data points needed from start date to present based on interval.
        ES: Calcula los puntos de datos necesarios desde la fecha de inicio hasta ahora.
        """
        num_str = ''.join([c for c in interval if c.isdigit()])
        unit_str = ''.join([c for c in interval if c.isalpha()])
        num = int(num_str) if num_str else 1
        t_time = unit_str

        now = datetime.datetime.now(datetime.UTC)
        delta = now - date
        total_minutes = delta.total_seconds() / 60

        if total_minutes < 0:
            raise ValueError("The start date is later than 'now'")

        if t_time == 'm':
            needed = total_minutes / num
        elif t_time == 'h':
            needed = (total_minutes / 60) / num
        elif t_time == 'd':
            needed = (total_minutes / (60 * 24)) / num
        elif t_time == 'w':
            needed = (total_minutes / (60 * 24 * 7)) / num
        elif t_time == 'mo':
            needed = (total_minutes / (60 * 24 * 30)) / num
        else:
            raise ValueError(f"Unsupported unit: {t_time}")

        return math.ceil(needed) + 1

    # -------------------   REAL TIME DATA   ---------------------

    class RealTimeStreamer:
        def __init__(self, client: dict, tickers: list, verbose: bool = False):
            """
            EN: Initializes the RealTimeStreamer.
            ES: Inicializa el RealTimeStreamer.
            """
            api_temp = TastyTradeAPI()

            self.dx_token = api_temp.DX_token(client)
            self.messages = api_temp.DX_messages(self.dx_token, tickers)
            self.link = api_temp.DX_link(client)

            self.verbose = verbose
            self.data = {}
            self.ws = None
            self.thread = None

        def _treat_data(self, message_data):
            try:
                data_block = message_data.get("data", [])
                if isinstance(data_block, list) and len(data_block) > 1:
                    event_type = data_block[0]
                    values = data_block[1]
                    # COMPACT format: values come in order specified in acceptEventFields
                    # For Quote with ["eventType", "eventSymbol", "bidPrice", "askPrice"]
                    # Data: [eventType, symbol, bidPrice, askPrice, eventType, symbol, ...]
                    if event_type == "Quote" and isinstance(values, list):
                        # 4 fields per quote
                        for i in range(0, len(values), 4):
                            if i + 3 < len(values):
                                symbol = values[i + 1]
                                bid = values[i + 2]
                                ask = values[i + 3]
                                if bid is not None and ask is not None:
                                    self.data[symbol] = (float(bid) + float(ask)) / 2
            except Exception as e:
                print("Error processing ws response -->", e)

        def _on_message(self, ws, message):
            if self.verbose:
                print("Message received -->", message)

            try:
                msg = json.loads(message)
            except Exception:
                return

            if msg.get("type") == "FEED_DATA":
                self._treat_data(msg)

        def _on_error(self, ws, error):
            print("Error -->", error)

        def _on_close(self, ws, *args):
            if self.verbose:
                print("-->Connection closed")

        def _on_open(self, ws):
            if self.verbose:
                print("-->Connection opened")

            ws.send(json.dumps(self.messages["SETUP"]))
            ws.send(json.dumps(self.messages["AUTH"]))
            ws.send(json.dumps(self.messages["CHANNEL"]))
            ws.send(json.dumps(self.messages["FEED"]))
            ws.send(json.dumps(self.messages["SUB"]))

        def start(self):
            self.ws = websocket.WebSocketApp(
                self.link,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            self.thread = threading.Thread(target=self.ws.run_forever, daemon=True)
            self.thread.start()
            time.sleep(1)
            return self

        def stop(self):
            if self.ws:
                self.ws.close()
            if self.thread:
                self.thread.join()

            if self.verbose:
                print("-->Streamer stopped")
