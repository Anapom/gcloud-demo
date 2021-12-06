import base64
import json
import os
from math import cos, sin, radians, sqrt
from google.cloud import firestore


client = firestore.Client(project='bigdata-anapom')

def subscribe(event, context):
    message = base64.b64decode(event['data']).decode('utf-8')
    print(f'data in function {message}')
    data = json.loads((message))
    node_id = int(data['node_id'])
    voltageA = float(data['voltageA'])
    voltageB = float(data['voltageB'])
    voltageC = float(data['voltageC'])
    print(f'voltage {voltageA} , {voltageB}, {voltageC}')
  
    # calculate unbalance voltage based on IEC
    PA = 0.0
    PB = 240.0
    PC = 120.0
    top_left  = voltageA * cos(radians(PA)) + voltageB * cos(radians(PB+240)) + voltageC * cos(radians(PC+120))
    top_right = voltageA * sin(radians(PA)) + voltageB * sin(radians(PB+240)) + voltageC * sin(radians(PC+120))
    top = sqrt((top_left * top_left) + (top_right *top_right))

    bot_left  = voltageA * cos(radians(PA)) + voltageB * cos(radians(PB+120)) + voltageC * cos(radians(PC+240))
    bot_right = voltageA * sin(radians(PA)) + voltageB * sin(radians(PB+120)) + voltageC * sin(radians(PC+240))
    bot = sqrt((bot_left * bot_left) + (bot_right *bot_right))

    unbalance = top / bot * 100

    if unbalance > 40:
        ret = "unbalance_voltage"
    else:
        ret = "normal_data"

    print(f'{node_id} status : {ret}')

    # save to firestore
    documentName = 'transformer' + str(node_id)
    doc = client.collection('trasnformerData').document(documentName)
    doc.set({
            'transformer_id': node_id,
            'status' : ret,
            'VoltageA' : voltageA,
            'VoltageB' : voltageB,
            'VoltageC' : voltageC
        })
