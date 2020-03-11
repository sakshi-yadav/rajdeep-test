import json
from elasticsearch import Elasticsearch
import paho.mqtt.client as mqttClient
from collections import defaultdict
from http import HTTPStatus
import pymongo as pymongo
import urllib.parse
import logging
import requests
from pymongo import database, MongoClient

error_codes = {
    1003: "Connection Not Established",
    1006: "id required",
    1007: "No record exists with this id",
}

def error_response(http_status, message_code):
    data=[]
    data.append(error_codes.get(message_code))
    return {
        "body": {
            "code": http_status,
            "message": "exception",
            "data": data
        },
        "statusCode": http_status,
        "headers": {'Content-Type': 'application/json'}
    }

def success_response(data, status_code):
    return {
        "body": {
        "code": 0,
        "message": "Request Completed Successfully",
        "data": data
        },
        "statusCode": status_code,
        "headers": {'Content-Type': 'application/json'}
        }

def connectToMongo(args,collection):
    doc = args.get("__ow_headers")
    mongo = doc.get("mongo")
    mongo = mongo.split("=")
    mongo = mongo.__getitem__(1)
    mongo_connect_url = mongo.split(",")
    mongo_connect_url = mongo_connect_url.__getitem__(0)
    try:
        database = pymongo.MongoClient(mongo_connect_url)
    except:
        return error_response(500, "cannot connnect to mongodb")
    database=database.get_database()
    connect=database[collection]
    return connect

def connectToElastic(args):
    headers=args.get("__ow_headers")
    url=headers.get("elastic_url")
    try:
        es = Elasticsearch(url)
        logging.info("Connection Established")
    except Exception as ex:
        logging.info("Cannot connect to Elastic Search" + ex)
    return es

def connectToMqtt(args):
    mqtt = args.get("__ow_headers").get("mqtt")
    mqtt=mqtt.split(",")
    user=mqtt.__getitem__(0)
    password=mqtt.__getitem__(1)
    broker=mqtt.__getitem__(2)
    port=int(mqtt.__getitem__(3))
    mqtt_client = mqttClient.Client()
    mqtt_client.username_pw_set(user, password=password)
    mqtt_client.connect(broker, port=port)
    mqtt_client.loop_start()
    return mqtt_client

def main(args):
    logging.basicConfig(format='%(message)s', level=logging.INFO)
    data= {
            "availableAttributes": [
                'energyConsumed'
            ],
            "units": [{
                'energyConsumed' : "KWh"
            }],
            "chartData": [
                {
                    "timeStamp": 1576539000000,
                    "energyConsumed": 56.7
                },
                {
                    "timeStamp": 1576625400000,
                    "energyConsumed": 29.4
                },
                {
                    "timeStamp": 1576711800000,
                    "energyConsumed": 34.0
                },
                {
                    "timeStamp": 1576798200000,
                    "energyConsumed": 14.0
                },
                {
                    "timeStamp": 1576884600000,
                    "energyConsumed": 68.0
                },
                {
                    "timeStamp": 1576971000000,
                    "energyConsumed": 42.0
                },
                {
                    "timeStamp": 1577057400000,
                    "energyConsumed": 64.0
                },
                {
                    "timeStamp": 1577143800000,
                    "energyConsumed": 38.0
                }
            ]
        }
    return success_response(data, 200)