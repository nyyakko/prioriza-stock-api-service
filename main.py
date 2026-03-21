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
import signal
import uuid

worker = {
    "config": {
        "host": os.getenv("SERVICE_HOST") or "0.0.0.0",
        "port": os.getenv("SERVICE_PORT") or "4000",
    },
    "services": {
        "rabbit": {
            "host": os.getenv("RABBIT_HOST"),
            "port": os.getenv("RABBIT_PORT")
        },
        "redis": {
            "host": os.getenv("REDIS_HOST"),
            "port": os.getenv("REDIS_PORT")
        }
    },
}

database = redis.Redis(host=worker["services"]["redis"]["host"], port=worker["services"]["redis"]["port"], db=0)

class StockMessagePublisher:
    def __init__(self, queueName):
        self.queueName = queueName
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=worker["services"]["rabbit"]["host"],
                port=worker["services"]["rabbit"]["port"],
                # NOTE: I'm disabling heartbeat entirely because its not
                # relevant to what i'm trying to achieve here
                heartbeat=0,
            ),
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queueName)

    def publish_request(self, tickers):
        id = random.randint(1000, 9999)
        body = json.dumps({
            "id": id,
            "data": tickers
        })
        self.channel.basic_publish(exchange="", routing_key=self.queueName, body=body)
        return id

    def stop(self): self.connection.close()

app = Flask(__name__)
publisher = StockMessagePublisher(queueName="stock-message-queue")

def signal_term_handler(_signum, _frame):
    print("[WARN] Terminating application...")
    sys.exit()

signal.signal(signal.SIGTERM, signal_term_handler)
signal.signal(signal.SIGINT, signal_term_handler)

@app.route("/health", methods=["GET"])
def health(): return { "status": "ok" }

@app.route("/stocks/lookup", methods=["POST"])
def get_stock():
    body = request.get_json()
    data = body.get("tickers")

    if not data or not isinstance(data, list): return { "error": "tickers array required" }, 400

    market = request.args.get("market")
    id = publisher.publish_request(
        data
            | Map(lambda param: param + (f".{market}" if market else ""))
            | Pipe(list)
    )

    return { "id": id, "status": "submitted" }, 202

@app.route("/stocks/lookup/<request_id>/status", methods=["GET"])
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
    corsOrigins = []

    try:
        with open("cors_origins.txt", "r") as file:
            for origin in file:
                corsOrigins.append(origin.strip())
    except:
        print("[WARN] cors_origins.txt not found!")

    CORS(app, resources={
        r"/api/*": {
            "origins": corsOrigins,
            "allow_headers": "*",
            "expose_headers": "*"
        }
    })

    serve(app, host=worker["config"]["host"], port=worker["config"]["port"])
    publisher.stop()
