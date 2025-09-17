# TastyTrade-API

## Simplifying Tastytrade & DXFeed integration in Python  

This project was created to simplify the process of developing **algorithmic trading software in Python** using the [Tastytrade broker](https://developer.tastytrade.com). While Tastytrade already provides an API, it is based on `REST` requests and `WebSocket` connections, which can be difficult to integrate directly into Python workflows. In particular, the **DXFeed workflow for data streaming** requires a complex `WebSocket` setup that is not for beginners.  

This library consolidates the most common interactions into a single, lightweight Python package — including **authentication**, **historical data**, **real-time market feeds**, **order execution**, and queries for **positions** and **transactions** — providing a simpler entry point to **automated trading** and **market analysis** in Python with Tastytrade.  

## First Stepts

### Installation

Clone this repository and install the required dependencies:

```bash
git clone https://github.com/gandpablo/TastyTrade-API
cd TastyTrade-API
pip install -r requirements.txt
```
---

### Configuration

To use this library you will need your **Tastytrade credentials** (`user` and `pass`), which are simply the **email** and **password** you use to log in on the Tastytrade website.  
In the examples provided, I load them through environment variables for convenience, but you can use them directly in your code.  

---

### Enabling API Access

You must also activate the **Open API Access** in your Tastytrade account before using this library:

1. Log in to [Tastytrade](https://tastytrade.com).  
2. Navigate to **Manage → My Profile → API**.  
3. Click **`Request Opt-In`** and accept the Terms & Conditions.  
4. Once enabled, your account will display **You are currently opted in**.

![Step 1](./images/api_optout.png)

---

### Session vs. Trading Account

Activating the API creates a **session**, not a full **trading account**: a session lets you query **market** and **historical data**, while for a trading account is required to **deposit funds** so yo can **place orders**, and manage **positions**. In the notebooks, only a session is used. You can have many accounts associated to the same session.


## Project Structure

```

TastyTradeAPI/
│
├── tastytrade/
│   ├── **init**.py
│   └── api.py                 # Main file: contains the TastyTradeAPI class and core methods
│
├── examples/
│   ├── example\_en.ipynb      # Jupyter Notebook in English – step-by-step usage examples
│   └── example\_es.ipynb      # Jupyter Notebook in Spanish – same examples, fully documented
│
├── requirements.txt           # Dependencies required to run the library
└── README.md

```

## Features & Examples

### 1. Login (Session)

To start, you need your Tastytrade `USER` and `PASS` (email and password).  
Once logged in, a **session** is created and you can use the API functions:

```python
from tastytrade.api import TastyTradeAPI

client = TastyTradeAPI(USER, PASS)
```

### 2. Account Information

You can query account details such as **balances**, **positions**, and **transactions**.  
Here is an example with the positions retrieving all of them (opened):

```python
positions = client.all_positions()
print(positions)
```
> Responde example

```json
[
  {
    "symbol": "AAPL",
    "quantity": 10,
    "averagePrice": 175.20,
    "marketPrice": 178.40,
    "unrealizedProfit": 32.0
  }
]
´´´















