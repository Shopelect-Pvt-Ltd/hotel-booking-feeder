import json
import requests
from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv
load_dotenv()
import time


# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

MONGO_URL = os.getenv('MONGO_URL')

client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")

def getGSTNFromPAN(pan):
    url = 'https://stage-apiplatform.finkraftai.com/api/auth/login'
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.7',
        'Authorization': 'Bearer null',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'https://mmtdemo.finkraftai.com',
        'Referer': 'https://mmtdemo.finkraftai.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Sec-GPC': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Brave";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'workspaceId': ''
    }
    payload = {
        "email": "mmt@finkraft.ai",
        "password": "finkr@ft"
    }
    token = None
    for i in range(1, 3, 1):
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            token = response.json()['data']['token']
            break
        time.sleep(1)
    if token is not None:
        url = 'https://stage-apiplatform.finkraftai.com/api/gstmeta/pangstins'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + str(token)
        }
        payload = {
            "pan": pan
        }
        for j in range(1, 3, 1):
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                gstdata = response.json()["data"]["gstinResList"]
                return gstdata
            time.sleep(1)

def getPans():
    db = client['bcd_hotel_booking']
    collection = db['bcd_client_details']
    distinct_pan_values = collection.distinct('pan')
    return distinct_pan_values

def getGSTIN(pans):
    db = client['bcd_hotel_booking']
    colletion = db['pan_to_gstins']
    for pan in pans:
        logging.info("Pan: "+str(pan))
        gstins = getGSTNFromPAN(pan)
        key_to_check = {"pan": pan}
        exiting_data = colletion.find_one(key_to_check)
        if exiting_data is None:
            gstinobj={}
            gstinobj["pan"]=pan
            gstinobj["gstins"]=gstins
            colletion.insert_one(gstinobj)

if __name__ == '__main__':
    pans=getPans()
    logging.info("PANS: "+str(pans))
    getGSTIN(pans)

