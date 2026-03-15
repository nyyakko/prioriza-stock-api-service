from flask_cors import CORS
from flask import Flask, request
from pipe21 import *
from typing import List
from waitress import serve
from yfinance import Tickers
import json
import os
import pika
import random
import redis
import uuid

RABBIT_HOST  = os.getenv("RABBIT_HOST")
RABBIT_PORT  = os.getenv("RABBIT_PORT")
REDIS_HOST   = os.getenv("REDIS_HOST")
REDIS_PORT   = os.getenv("REDIS_PORT")
SERVICE_HOST = os.getenv("SERVICE_HOST") or "0.0.0.0"
SERVICE_PORT = os.getenv("SERVICE_PORT") or "4000"

class StockMessagePublisher:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBIT_HOST,
                port=RABBIT_PORT,
                # NOTE: I'm disabling heartbeat entirely because its not
                # relevant to what i'm trying to achieve here
                heartbeat=0,
            ),
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="stock-message-queue")

    def publish_request(self, tickers):
        id = random.randint(1000, 9999)
        body = json.dumps({
            "id": id,
            "data": tickers
        })
        self.channel.basic_publish(exchange="", routing_key="stock-message-queue", body=body)
        return id

    def close(self):
        self.connection.close()

app       = Flask(__name__)

publisher = StockMessagePublisher()
database  = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

@app.route("/api/health", methods=["GET"])
def health(): return { "status": "ok" }

@app.route("/api/stocks/lookup", methods=["POST"])
def get_stock():
    body = request.get_json()
    data = body.get("tickers")

    if not data or not isinstance(data, list): return { "error": "tickers array required" }, 400

    market = request.args.get("market")

    tickers = (
        data
            | Map(lambda param: param + (f".{market}" if market else ""))
            | Pipe(list)
    )
    id = publisher.publish_request(tickers)
    entry = { "id": id, "status": "submitted" }
    database.set(id, json.dumps({ **entry, "result": None }))

    return entry, 202

@app.route("/api/stocks/lookup/<request_id>/status", methods=["GET"])
def get_stock_status(request_id):
    data = database.get(request_id)
    if data is None: return { "error": "lookup request not found" }, 404
    result = json.loads(data)
    return {
        "id": result["id"],
        "status": result["status"],
        "result": json.loads(result["result"]) if result["status"] == "finished" else None
    }

if __name__ == "__main__":
    CORS(app, resources={
        r"/api/*": {
            "origins": "http://localhost:5173",
            "allow_headers": "*",
            "expose_headers": "*"
        }
    })
    serve(app, host=SERVICE_HOST, port=SERVICE_PORT)
    publisher.close()
