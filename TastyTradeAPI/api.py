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

        self._API_URL = 'https://api.tastyworks.com'


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
        

    def _get_accounts(self,session_token: str) -> list:

        """
        EN: Obtains the list accounts associated with the client.

        ES: Obtiene la lista de cuentas asociadas al cliente.
        """

        url = f"{self._API_URL}/customers/me/accounts"

        headers = {
            "Authorization": session_token
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code in [200,201]:
            data = response.json()
            data = data['data']['items']
            return data
        else:
            raise Exception(f"Error retrieving account data| Status Code: {response.status_code}")

    
    def _account_number(self,session_token: str,num: int = 0 ) -> str:

        """
        EN: Obtains the account number based on the provided index, default is 0 (the user's first account).

        ES: Obtiene el número de cuenta basado en el indice proporcioando, por defecto 0 (la primera cuenta del usuario).
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
    
    def _get_client_info(self,session_token: str) -> dict:

        """
        EN: Obtains general information about the client (no account).

        ES: Obtiene información general sobre el cliente (no de la cuenta).
        """

        url = f"{self._API_URL}/customers/me"

        headers = {
            "Authorization": session_token
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code in [200,201]:
            data = response.json()
            data = data['data']
            return data
        else:
            raise Exception(f"Error retrieving general data| Status Code: {response.status_code}")
    
    # --------------------------------------------------------------
        
        
    # -------------------   ES: INICIALIZAR CLIENTE   -------------------
    # -------------------   EN: INITIALIZE CLIENT   ---------------------
    
    
    def Client(self,USER: str,PASS: str,num: int = 0) -> dict:

        """
        EN: Creates a TastyTrade client with USER and PASS and the account returns a dictionary with session_token and account_number. It is requiered to use all the functions.

        ES: Crea un cliente de TastyTrade con USER y PASS y la cuenta devuelve un diccionario con session_token y account_number. Se requiere para usar todas las funciones.
        """

        self._USER = USER
        self._PASS = PASS

        session_token = self._get_token()
        ac_num = self._account_number(session_token,num)

        self._Client = {'session_token':session_token,'account_number':ac_num}

        return {'session_token':session_token,'account_number':ac_num}
    
    # --------------------------------------------------------------
    
    
    def _get__balances(self,Client: dict) -> dict:

        """
        EN: Obtains the general info of the account associated to the client (balances).

        ES: Obtiene la información general de la cuenta asociada al cliente (balances).
        """

        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client. Most functions will not work.")

        url = f"{self._API_URL}/accounts/{account_number}/balances"

        headers = {
            "Authorization": session_token
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code in [200,201]:
            data = response.json()
            data = data['data']
            return data
        
        else:
            raise Exception(f"Error retrieving balances| Status Code: {response.status_code}")
        
        
    # -------------------   ES: MÉTRICAS DE LA CUENTA   -------------------
    # -------------------   EN: ACCOUNT METRICS   -------------------
        
        
    def client_info(self,Client: dict) -> dict:

        """
        EN: Obtains basic information about the client (name, email, citizenship country and liquid net worth).

        ES: Obtiene información básica sobre el cliente (nombre, email, país de ciudadanía y patrimonio neto líquido).
        """
        
        data = self._get_client_info(Client['session_token'])

        return {
            "name": f"{data.get('first-name', '')} {data.get('last-name', '')}".strip(),
            "email": data.get("email", ""),
            "citizenship_country": data.get("citizenship-country", ""),
            "liquid_net_worth": data.get("customer-suitability", {}).get("liquid-net-worth")}
        
    
    def balance(self,Client: dict) -> float:

        """
        EN: Obtains the cash balance of the account associated to the client.

        ES: Obtiene el balance de efectivo de la cuenta asociada al cliente.
        """
        
        data = self._get__balances(Client)

        try:
            return float(data['cash-balance'])
       
        except Exception as e:
            raise Exception(f"Error retrieving balances --->\n{e}")
        
    
    def equity_BP(self,Client: dict) -> float:

        """
        EN: Obtains the equity buying power of the account associated to the client.

        ES: Obtiene el equity buying power de la cuenta asociada al cliente.
        """

        data = self._get__balances(Client)

        try:
            return float(data['equity-buying-power'])
       
        except Exception as e:
            raise Exception(f"Error retrieving Equity Buying Power --->\n{e}")
    

    def derivative_BP(self,Client: dict) -> float:

        """
        EN: Obtains the derivative buying power of the account associated to the client.

        ES: Obtiene el derivative buying power de la cuenta asociada al cliente.
        """

        data = self._get__balances(Client)

        try:
            return float(data['derivative-buying-power'])
       
        except Exception as e:
            raise Exception(f"Error retrieving Derivative Buying Power --->\n{e}")
        

    def liquidity(self,Client: dict) -> float:

        """
        EN: Obtains the liquidity of the account associated to the client.

        ES: Obtiene la liquidity de la cuenta asociada al cliente.
        """

        data = self._get__balances(Client)

        try:
            return float(data['net-liquidating-value'])
       
        except Exception as e:
            raise Exception(f"Error retrieving Equity Buying Power --->\n{e}")
        
        
    def total_fees(self,Client: dict) -> float:

        """
        EN: Obtains the total fees applied to the account associated to the client.

        ES: Obtiene los fees totales aplicados a la cuenta asociada al cliente.
        """

        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client. Most functions will not work.")

        url = f"{self._API_URL}/accounts/{account_number}/transactions/total-fees"

        headers = {
            "Authorization": session_token
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code in [200,201]:
            data = response.json()
            data = data['data']['total-fees']
            return data
        
        else:
            raise Exception(f"Error retrieving the applied fees| Status Code: {response.status_code}")   

    # --------------------------------------------------------------
        
    # -------------------   ES: EJECUTAR ORDENES   -------------------
    # -------------------   EN: PLACE ORDERS   -------------------

    def _start_long(self,ticker: str,val: int,time_force: str, otype: str) -> str:

        """
        EN: Json to create a market order to open a long position (buy to open).

        ES: Json para crear una orden de mercado para abrir una posición larga (compra para abrir).
        """
    
        order = {
            "time-in-force": time_force,
            "order-type": otype,
            "legs": [
                {
                    "instrument-type": "Equity",
                    "symbol": ticker,
                    "quantity": val,
                    "action": "Buy to Open"
                }
            ]
        }
        
        return json.dumps(order)

    def _end_long(self,ticker: str,val: int,time_force: str, otype: str) -> str:

        """
        EN: Json to create a market order to close a long position (sell to close).

        ES: Json para crear una orden de mercado para cerrar una posición larga (venta para cerrar).
        """
        
        order = {
            "time-in-force": time_force,
            "order-type": otype,
            "legs": [
                {
                    "instrument-type": "Equity",
                    "symbol": ticker,
                    "quantity": val,
                    "action": "Sell to Close"
                }
            ]
        }
        
        return json.dumps(order)
    
    def _start_short(self,ticker: str,val: int,time_force: str, otype: str) -> str:

        """
        EN: Json to create a market order to open a short position (sell to open).

        ES: Json para crear una orden de mercado para abrir una posición corta (venta para abrir).
        """
    
        order = {
            "time-in-force": time_force,
            "order-type": otype,
            "legs": [
                {
                    "instrument-type": "Equity",
                    "symbol": ticker,
                    "quantity": val,
                    "action": "Sell to Open"
                }
            ]
        }
        
        return json.dumps(order)

    def _end_short(self,ticker: str,val: int,time_force: str, otype: str) -> str:

        """
        EN: Json to create a market order to close a short position (buy to close).

        ES: Json para crear una orden de mercado para cerrar una posición corta (compra para cerrar).
        """
    
        order = {
            "time-in-force": time_force,
            "order-type": otype,
            "legs": [
                {
                    "instrument-type": "Equity",
                    "symbol": ticker,
                    "quantity": val,
                    "action": "Buy to Close"
                }
            ]
        }
        
        return json.dumps(order)


    def order(self,Client: dict,ticker: str,val: int,order_type: str,action: str,time_force: str = 'Day', otype: str = 'Market') -> dict:

        """
        EN: Places a market order for the specified ticker and the quantity (val). You must indicate if it is a Long or Short order and if you want to Start or End the position.
            It returns a dictionary with the response of the order (status, size, fees, comissions, new buying power and date).
        
        ES: Realiza una orden de mercado para el ticker especificado y la cantidad (val). Debes indicar si es una orden Long o Short y si quieres Start o End la posición.
            Devuelve un diccionario con la respuesta de la orden (status, size, fees, comissions, new buying power and date).
        """

        if order_type == 'Long':
            if action == 'Start':
                order = self._start_long(ticker,val,time_force,otype)
            elif action == 'End':
                order = self._end_long(ticker,val,time_force,otype)
            else:
                raise Exception(f"Order error ---> Invalid action, try: Start or End")
        
        elif order_type == 'Short':
            if action == 'Start':
                order = self._start_short(ticker,val,time_force,otype)
            elif action == 'End':
                order = self._end_short(ticker,val,time_force,otype)
            else:
                raise Exception(f"Order error ---> Invalid action, try: Start or End")
        
        else:
            raise Exception(f"Order error ---> Invalid order type, try: Long or Short")


        session_token = Client['session_token']
        account_number = Client['account_number']  

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client. Orders cannot be executed.")

        url = f"{self._API_URL}/accounts/{account_number}/orders"
        
        headers = {
            "Authorization": session_token,
            'Content-Type': 'application/json'
            
        }
        
        response = requests.post(url, headers=headers, data=order)

        if response.status_code not in [200,201]:
            raise Exception(f"Order error ---> Response Status: {response.status_code}")

        data = response.json()

        ord = data["data"]["order"]
        fees = data["data"]["fee-calculation"]
        bp = data["data"]["buying-power-effect"]

        return {
            "Status": ord["status"],
            "Size": ord["size"],
            "Fees": float(fees["total-fees"]),
            "Commission": float(fees["commission"]),
            "NewBuyingPower": float(bp["new-buying-power"]),
            "ReceivedAt": ord["received-at"]
        }


    # --------------------------------------------------------------
        

    # ------------------- ES: DATOS DE LAS POSICIONES -------------------
    # ------------------- EN: POSITION DATA -------------------
    
    def _position(self,Client:dict) -> list:

        """
        EN: Obtains the list of open positions associated with the account of the client. Cada posición es un diccionario de información.

        ES: Obtiene la lista de las posiciones abiertas asociadas a la cuenta del cliente. Each position is a dictionary of information.
        """

        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client. Most functions will not work.")

        url = f"{self._API_URL}/accounts/{account_number}/positions"

        headers = {
            "Authorization": session_token
        }
            
        response = requests.get(url, headers=headers)
            
        if response.status_code in [200,201]:
            data = response.json()
            data = data['data']['items']
            return data
            
        else:
            raise Exception(f"Error retrieving positions| Status Code: {response.status_code}")

    def _parse_position_data(self,data: dict) -> dict:

        """
        EN: Parses the position data to return a more user-friendly dictionary. También hace algunos calculos para obtener el profit.

        ES: Parsea los datos de la posición para devolver un diccionario más amigable para el usuario. Also does some calculations to get the profit.
        """

        def format_date(date_str: str) -> str:

            """
            EN: Formats the date string to a more readable format.
            ES: Formatea la cadena de fecha a un formato más legible.
            """

            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
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
            profit = round((current_price - avg_entry_price) * quantity * multiplier,3)
        elif direction and direction.lower() == "short":
            profit = round((avg_entry_price - current_price) * quantity * multiplier,3)
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

    
    def all_positions(self,Client: dict) -> dict:

        """
        EN: Obtains all open positions associated with the account of the client. Returns a dictionary with the symbol as key and the parsed position data as value.

        ES: Obtiene todas las posiciones abiertas asociadas a la cuenta del cliente. Devuelve un diccionario con el símbolo como clave y los datos de la posición parseados como valor.
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
                    pos[parsed['symbol']] = {"error": str(e)}
            
            return pos

        except Exception as e:
            raise Exception(f"Error parsing positions --->\n{e}")
    

    def check_position(self,Client: dict,symbol: str) -> dict:

        """
        EN: Checks if there is an open position for the given symbol. Returns the parsed position data if found, otherwise returns None.

        ES: Comprueba si hay una posición abierta para el símbolo dado. Devuelve los datos de la posición parseados si se encuentra, de lo contrario devuelve None.
        """

        positions = self.all_positions(Client)

        if symbol in positions:
            return positions[symbol]
        else:
            return None

    # --------------------------------------------------------------
        
    # ------------------- ES: DATOS DE LAS TRANSACCIONES -------------------
    # ------------------- EN: TRANSACTION DATA -------------------
    
    def _transaction(self,Client: dict) -> list:

        """
        EN: Obtains the list of transactions associated with the account of the client. Each transaction is a dictionary of information.

        ES: Obtiene la lista de transacciones asociadas a la cuenta del cliente. Cada transacción es un diccionario de información.
        """

        session_token = Client['session_token']
        account_number = Client['account_number']

        if account_number == 'No Account':
            raise Exception("Warning: No account is associated with this client. Most functions will not work.")

        url = f"{self._API_URL}/accounts/{account_number}transactions"

        headers = {
            "Authorization": session_token
        }
            
        response = requests.get(url, headers=headers)
            
        if response.status_code in [200,201]:
            data = response.json()
            data = data['data']['items']
            return data
            
        else:
            raise Exception(f"Error retrieving transactions| Status Code: {response.status_code}")


    def _parse_transaction(self, item: dict) -> dict:

        """
        EN: Parses the transaction data to return a more user-friendly dictionary. 

        ES: Parsea los datos de la transacción para devolver un diccionario más amigable para el usuario.
        """

        def format_date(date_str: str) -> str:

            """
            EN: Formats the date string to a more readable format.

            ES: Formatea la cadena de fecha a un formato más legible.
            """

            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
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

    def all_transactions(self,Client: dict) -> dict:

        """
        EN: Obtains all transactions associated with the account of the client. Returns a dictionary with the symbol as key and a list of parsed transaction from that symbol as value.

        ES: Obtiene todas las transacciones asociadas a la cuenta del cliente. Devuelve un diccionario con el símbolo como clave y una lista de transacciones parseadas de ese símbolo como valor.
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
                try:
                    symbol = t.get("symbol")
                    if symbol not in data:
                        data[symbol] = []
                    data[symbol] = {"error": str(e)}
                except:
                    pass
        
        return data

    def check_transaction(self,Client: dict,symbol: str) -> dict:

        """
        EN: Checks if there are transactions for the given symbol. Returns a list of parsed transaction data if found, otherwise returns None.

        ES: Comprueba si hay transacciones para el símbolo dado. Devuelve una lista de datos de transacciones parseadas si se encuentra, de lo contrario devuelve None.
        """

        transactions = self.all_transactions(Client)

        if symbol in transactions:
            return transactions[symbol]
        else:
            return None
        
    # -------------------------------------------------------------

    def still_connected(self,Client):

        session_token = Client['session_token']
        try:
            _ = self._get_client_info(session_token)
            return True
        except:
            return False
        
    # -------------------------------------------------------------

    # -------------------  MARKET DATA CON DX FEED   ---------------
    # -------------------  MARKET DATA WITH DX FEED ---------------
    
    def _get_DX_vals(self,Client: dict) -> dict:

        """
        EN: Obtains the token to authorize in the DX feed API (data provider).

        ES: Obtienes el token para autorizarte en la API de feed DX (proveedor de datos).
        """

        session_token = Client['session_token']
    
        url = f"{self._API_URL}/api-quote-tokens"
        headers = {
            "Authorization": session_token
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code in [200,201]:
            data = response.json()
            return data['data']
        else:
            raise Exception(f"Error retrieving DX session | Status Code: {response.status_code}")
    
    
    def DX_link(self,Client: dict) -> str:
       
       """
       EN: Obtains the url api to DX feed.

       ES: Obtienes la url de la api para DX feed.
       """
       
       data = self._get_DX_vals(Client)

       try:
           return data['dxlink-url']
       
       except Exception as e:
            raise Exception(f"Error retrieving DX LINK --->\n{e}")
       
    def DX_token(self,Client: dict) -> str:
       
       """
       EN: In case you want to know the DX token.

       ES: Por si acaso quieres conocer el token de DX.
       """
       
       data = self._get_DX_vals(Client)

       try:
           return data['token']
       
       except Exception as e:
            raise Exception(f"Error retrieving DX TOKEN --->\n{e}")
    
    def DX_messages(self,dx_token: str,ticker_list: list) -> dict:

        """
        EN: Creates the necessary messages to communicate with the DX websocket to get real-time data.
            Important --> These are persistent connections for real-time data, not historical data.

        ES: Crea los mensajes necesarios para comunicarse con el websocket de DX para obtener los datos en tiempo real. 
            Importante --> Son conexiones persistentes para datos en tiempo real, no historicos.
        """

        if len(ticker_list) == 0:
            return None

        lista = []
        for i in ticker_list:
            val = {"type":"Quote","symbol":i}
            lista.append(val)

        messages = {
        'SETUP':{"type": "SETUP", "channel": 0, "version": "0.1-DXF-JS/0.3.0", "keepaliveTimeout": 120, "acceptKeepaliveTimeout": 120},
        'AUTH':{"type": "AUTH", "token": dx_token, "channel": 0},
        'CHANNEL':{"type": "CHANNEL_REQUEST", "channel": 3, "service": "FEED", "parameters": {"contract": "AUTO"}},
        'FEED':{"type": "FEED_SETUP", "channel": 3, "acceptAggregationPeriod": 0.1, "acceptDataFormat": "FULL", "acceptEventFields": {"Quote": ["askPrice", "eventSymbol"]}},
        'SUB':{"type": "FEED_SUBSCRIPTION", "channel": 3, "reset": True, "add": lista},
        'KEEP':{"type": "KEEPALIVE", "channel": 0}
        }

        return messages
    
    def _get_hist_DX_messages(self,dx_token: str,ticker_list: list,num: int,t_time: str,
                              fromTime ,wait_time: int = 10,vars: list = ['close','eventSymbol','time']) -> dict:
        
        """
        EN: Creates the necessary messages to communicate with the DX websocket to get historical data.
            Important --> These are not persistent connections, they close after getting the historical data.
            You can only get data from the indicated date (with fromTime) to the present, you cannot get intervals of dates.


        ES: Crea los mensajes necesarios para comunicarse con el websocket de DX para obtener los datos historicos.
            Importante --> No son conexiones persistentes, se cierran tras obtener los datos historicos.
            Solo se pueden desde la fecha indicada (con el fromTime) hasta la actualidad, no se pueden intervalos de fechas.
        """

        if len(ticker_list) == 0:
            return None

        lista = []
        for i in ticker_list:
            s = f"{i}{{={num}{t_time}}}"
            val = {"type":"Candle","symbol":s,"fromTime": fromTime,'tho':True,'a':'s'}
            lista.append(val)

        messages = {
        'SETUP':{"type": "SETUP", "channel": 0, "version": "0.1-DXF-JS/0.3.0", "keepaliveTimeout": wait_time, "acceptKeepaliveTimeout": wait_time},
        'AUTH':{"type": "AUTH", "token": dx_token, "channel": 0},
        'CHANNEL':{"type": "CHANNEL_REQUEST", "channel": 3, "service": "FEED", "parameters": {"contract": "AUTO"}},
        'FEED':{"type": "FEED_SETUP", "channel": 3, "acceptAggregationPeriod": 0.1, "acceptDataFormat": "FULL", "acceptEventFields": {"Candle": vars}},
        'SUB':{"type": "FEED_SUBSCRIPTION", "channel": 3, "reset": True, "add": lista},
        'KEEP':{"type": "KEEPALIVE", "channel": 0}
        }

        return messages
        
    # --------------------------------------------------------------


    # -------------------   ES: WEBSOCKET PARA DATOS HISTORICOS   -------------------
    # -------------------   EN: WEBSOCKET FOR HISTORICAL DATA   ---------------------

    def _websocket_historical(self,link:str ,messages: dict ,df: pd.DataFrame ,vars: list) -> pd.DataFrame:

        """
        EN: Connects to the DX websocket and retrieves historical data, appending it to the provided DataFrame.

        ES: Se conecta al websocket de DX y recupera datos históricos, agregándolos al DataFrame proporcionado.
        """

        def add_rows_to_df(existing_df, iter):
            
            for i in range(0, len(iter), vars):
                triple = iter[i:i+vars]  
                row = triple
                existing_df.loc[len(existing_df)] = row

        def treat_message(ws, message, messages,df):
            message_data = json.loads(message)
            if message_data.get("type") == "AUTH_STATE" and message_data.get("state") == 'UNAUTHORIZED':
                ws.send(json.dumps(messages['AUTH']))
            elif message_data.get("type") == "AUTH_STATE" and message_data.get("state") == 'AUTHORIZED':
                ws.send(json.dumps(messages['CHANNEL']))
            elif message_data.get("type") == "CHANNEL_OPENED" and message_data.get("channel") == 3:
                ws.send(json.dumps(messages['FEED']))
            elif message_data.get("type") == "FEED_CONFIG" and message_data.get("channel") == 3:
                ws.send(json.dumps(messages['SUB']))
            elif message_data.get("type") == "FEED_DATA" and message_data.get("channel") == 3:
                    
                data = message_data['data'][1]
                add_rows_to_df(df, data)

        def on_message(ws, message):
            treat_message(ws, message, messages,df)

        def on_error(ws, error):
            print('Error: -->', error)

        def on_close(ws, *args):
            pass

        def on_open(ws):
            ws.send(json.dumps(messages['SETUP']))

        
        ws = websocket.WebSocketApp(link,
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.run_forever()

        return df
    
    # -------------------   ES: DATOS HISTÓRICOS   -------------------
    # -------------------   EN: HISTORICAL DATA   -------------------



    def get_historical(self, Client: dict, tickers: list, interval: str, vars: list = ['close'], max_data: int =100) -> dict:

        """
        EN: Obtains historical data for the specified tickers and interval. Returns a dictionary with the ticker as key and a DataFrame with the historical data as value.

        ES: Obtiene datos históricos para los tickers e intervalo especificados. Devuelve un diccionario con el ticker como clave y un DataFrame con los datos históricos como valor.
        """

        link = self.DX_link(Client)
        token = self.DX_token(Client)
        num = int(interval[0])
        t_time = interval[1]

        if len(tickers) == 0:
            return {}
        
        if len(vars) == 0:
            vars.append('close')

        vars.append('eventSymbol')
        vars.append('time')

        days = int((max_data * num)/6)

        if t_time == 'd':
            days = int((max_data * num))

        if days > 12 or len(tickers) > 6:
            wait_time = 20
        
        max_rows = int(max_data)


        now = datetime.datetime.now(datetime.UTC)

        fromTime = int((now - timedelta(hours=days)).timestamp() * 1000)

        messages = self._get_hist_DX_messages(token, tickers, num, t_time, fromTime,wait_time,vars)

        vars_names = {
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume',
            'eventSymbol': 'Ticker',
            'time': 'Time'
        }

        cols = [vars_names[v] for v in vars if v in vars_names]
        df = pd.DataFrame(columns = cols)

        df_new = self._websocket_historical(link,messages,df,len(vars))
    
        def treat_tck(tck):
            tck = str(tck)
            cad = tck.split('{')[0]
            return cad
        
        def dividir_por_empresa(df,max_rows):
            diccionario_empresas = {}
            for empresa in df['Ticker'].unique():
                df_empresa = df[df['Ticker'] == empresa]
                df_empresa = df_empresa.drop(columns = ['Ticker'])
                df_empresa = df_empresa.head(max_rows)
                diccionario_empresas[empresa] = df_empresa
                
            return diccionario_empresas
        
            
        df_new['Ticker'] = df_new['Ticker'].apply(treat_tck)
        df_new['Time'] = df_new['Time'] / 1000
        df_new['Date'] = pd.to_datetime(df_new['Time'], unit='s')
        df_new = df_new.drop(columns = ['Time'])
        
        df_new = df_new[((df_new['Date'].dt.hour > 15) | ((df_new['Date'].dt.hour == 15) & (df_new['Date'].dt.minute >= 30))) & 
                ((df_new['Date'].dt.hour < 21) | ((df_new['Date'].dt.hour == 21) & (df_new['Date'].dt.minute <= 30)))]
        
        
        dicc = dividir_por_empresa(df_new,max_rows)

        return dicc
    

    def values_from_data(self, interval: str, date: str) -> int:

        """
        EN: Calculates the number of data points needed from a given start date to the present based on the specified interval.

        ES: Calcula el número de puntos de datos necesarios desde una fecha de inicio dada hasta la actualidad en función del intervalo especificado.
        """

        num_str = ''.join([c for c in interval if c.isdigit()])
        unit_str = ''.join([c for c in interval if c.isalpha()])
        num = int(num_str)
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
            needed = (total_minutes / (60*24)) / num
        elif t_time == 'w':
            needed = (total_minutes / (60*24*7)) / num
        elif t_time == 'mo':
            needed = (total_minutes / (60*24*30)) / num
        else:
            raise ValueError(f"Unidad no soportada: {t_time}")

        return math.ceil(needed) + 1
    

    # -------------------   ES: DATOS TIEMPO REAL   -------------------
    # -------------------   EN: REAL TIME DATA   ---------------------

    class RealTimeStreamer:
        def __init__(self, client: dict,tickers: list, verbose: bool = False):

            """
            EN: Initializes the RealTimeStreamer with the provided client, tickers, and verbosity setting.

            ES: Inicializa el RealTimeStreamer con el cliente, tickers y configuración de verbosidad proporcionados.
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
                    if data_block[0] == "Quote":
                        values = data_block[1]
                        if len(values) % 2 == 0:
                            for i in range(0, len(values), 2):
                                price = values[i]
                                symbol = values[i+1]
                                self.data[symbol] = float(price)
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








    

            
    
