import json
import requests
from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
import time
from state_mapping_details import state_details

# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

MONGO_URL = os.getenv('MONGO_URL')

client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")


def getGSTNFromPAN(pans):
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
    pantogstmap = {}
    if token is not None:
        url = 'https://stage-apiplatform.finkraftai.com/api/gstmeta/pangstins'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + str(token)
        }
        for i in range(len(pans)):
            payload = {
                "pan": pans[i]
            }

            for j in range(1, 3, 1):
                response = requests.post(url, headers=headers, json=payload)

                if response.status_code == 200:
                    gstdata = response.json()["data"]["gstinResList"]

                    gstdatadict = {}
                    for k in range(len(gstdata)):
                        gstdatadict[gstdata[k]["stateCd"]] = gstdata[k]
                    pantogstmap[pans[i]] = gstdatadict
                    break
                time.sleep(1)
        return pantogstmap
def get_state_and_short_name_from_pincode(pincodes):
    pintostatedetail={}
    for pincode in pincodes:
        url = f"https://api.postalpincode.in/pincode/{pincode}"
        for i in range(1,3,1):
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data[0]['Status'] == 'Success':
                    state = data[0]['PostOffice'][0]['State']
                    details = state_details.get(state, {"short_name": "Unknown", "state_code": "Unknown"})
                    short_name = details["short_name"]
                    state_code = details["state_code"]
                    pintostatedetail[pincode]={"state":state, "short_name":short_name,"state_code":state_code}
                    break
            time.sleep(1)
    return pintostatedetail

def getCustomerDetails():
    failed=[]
    db = client['bcd_hotel_booking']
    colletion = db['bcd_client_details']
    bcd_client_details=list(colletion.find({"gstin": {"$exists": False}}))
    pans=set()
    pincodes = set()
    for data in bcd_client_details:
        pans.add(data['pan'])
        pincodes.add(data['address_information'].split(" ")[2])

    pintostatedetail=get_state_and_short_name_from_pincode(list(pincodes))
    logging.info(pintostatedetail)
    pantogstmap=getGSTNFromPAN(list(pans))
    logging.info(pantogstmap)
    if pintostatedetail!=None and pintostatedetail!=None:
        for data in bcd_client_details:
            if "gstin" not in data:
                pan=data["pan"]
                pincode=data['address_information'].split(" ")[2]
                try:
                    state_details=pintostatedetail[pincode]
                    state_code=state_details["state_code"]
                    state_short_name=state_details["short_name"]
                    state=state_details["state"]

                    logging.info("_id: "+str(data["_id"]))
                    logging.info("customer_code: "+str(data["customer_code"]))
                    logging.info("pan: "+str(pan))
                    logging.info("state_code: "+str(state_code))

                    gstin_details=pantogstmap[pan][state_code]
                except:
                    failed.append(data["customer_code"])
                    continue
                gstin=gstin_details['gstin']
                # authStatus=gstin_details['authStatus']

                key_to_check={"customer_code":data["customer_code"]}
                result = colletion.update_one(
                    key_to_check,
                    {
                        "$set": {
                            "gstin": gstin,
                            "state":state,
                            "state_short_name":state_short_name,
                            "state_code": state_code,
                        }
                    })
                if result.matched_count > 0:
                    logging.info("Updated the document for customer_code: " + str(data["customer_code"]))
    logging.info("Failed Id: "+str(failed))

if __name__ == '__main__':
    getCustomerDetails()
    # get_state_and_short_name_from_pincode(["520010"])