import json
import requests
from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv

load_dotenv()
import time
import psycopg2
from datetime import datetime, timezone
import pytz

ist = pytz.timezone('Asia/Kolkata')
# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

MONGO_URL = os.getenv('MONGO_URL')

client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")
SENDGRID_KEY = os.getenv("SENDGRID_API_KEY")

postgres_host = os.getenv("PG_HOST")
postgres_db = os.getenv("PG_DATABASE")
postgres_user = os.getenv("PG_USER")
postgres_password = os.getenv("PG_PASSWORD")
postgres_port = os.getenv("PG_PORT")

DB_CONFIG = {
    'host': postgres_host,
    'port': postgres_port,
    'dbname': postgres_db,
    'user': postgres_user,
    'password': postgres_password
}

def sendAlert():
    sender_email = "info@finkraft.ai"
    recipient_email = ["komalkant@kgrp.in","tabrez@kgrp.in"]
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now.astimezone(ist)
    content = "Failed to update the GST Data for BCD hotel booking " + str(ist_now.strftime('%Y-%m-%d %H:%M:%S'))
    subject = "Failed to update the GST Data for BCD hotel booking " + str(ist_now.strftime('%Y-%m-%d %H:%M:%S'))

    url = "https://api.sendgrid.com/v3/mail/send"
    headers = {
        "Authorization": f"Bearer {SENDGRID_KEY}",
        "Content-Type": "application/json"
    }
    recipient_email_list = []
    for i in range(len(recipient_email)):
        recipient_email_list.append({"email": recipient_email[i]})

    data = {
        "personalizations": [
            {
                "to": recipient_email_list,
                "subject": subject
            }
        ],
        "from": {"email": sender_email},
        "content": [
            {
                "type": "text/html",
                "value": content
            }
        ]
    }

    try:
        for i in range(1, 4, 1):
            response = requests.post(url, headers=headers, json=data, verify=False)
            print(response.status_code)
            if response.status_code == 202:
                print("Email sent successfully!")
                break
            time.sleep(5)
    except Exception as e:
        print("Error sending email:", e)


def getPans():
    db = client['bcd_hotel_booking']
    collection = db['bcd_client_details']
    distinct_pan_values = collection.distinct('pan')
    return distinct_pan_values


def getGstinfo(authtoken, gstin):
    url = 'https://publicservices.gst.gov.in/publicservices/auth/api/search/tp'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'application/json',
        'Cookie': 'AuthToken=' + authtoken,
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/services/auth/searchtpbypan',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'at': authtoken,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }

    payload = {
        'gstin': gstin
    }
    try:
        for i in range(1, 3, 1):
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                return response.json()
            time.sleep(5)
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
        return None
    except Exception as err:
        print("An error occurred: %s", err)
        return None


# Function to call the first API with PAN number
def getGSTListFromPan(authtoken, pan_number):
    # Define the API endpoint and headers for the first API call
    url = 'https://services.gst.gov.in/services/auth/api/get/gstndtls'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'application/json',
        'Cookie': 'AuthToken=' + authtoken,
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/services/auth/searchtpbypan',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'at': authtoken,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
    data = json.dumps({"panNO": pan_number})
    for i in range(1, 3, 1):
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()['gstinResList']
        time.sleep(5)
    return None


# Function to read token from the database
def get_token(cursor):
    cursor.execute("SELECT token FROM tokens")
    token = cursor.fetchone()
    if token:
        return token[0]
    else:
        return None


def getPlaceOfBussinessinfo(authtoken, gstin):
    url = 'https://publicservices.gst.gov.in/publicservices/auth/api/search/tp/busplaces'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'application/json',
        'Cookie': 'AuthToken=' + authtoken,
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/services/auth/searchtpbypan',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'at': authtoken,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }

    payload = {
        'gstin': gstin
    }
    try:
        for i in range(1, 3, 1):
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                return response.json()
            time.sleep(5)
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
        return None
    except Exception as err:
        print("An error occurred: %s", err)
        return None


if __name__ == '__main__':
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        db = client['bcd_hotel_booking']
        # colletion = db['pan_to_gstins_test']
        colletion = db['pan_to_gstins']

        pans = getPans()
        logging.info("PANS: " + str(pans))
        finaldata=[]
        for i in range(len(pans)):
            pan = pans[i]
            token = get_token(cursor)
            gstinobj = {}
            gstinobj["pan"] = pan
            gstlist = getGSTListFromPan(token, pan)
            if gstlist is None:
                sendAlert()
                exit(0)
            logging.info(gstlist)
            newlist = []
            for j in range(len(gstlist)):
                gst = gstlist[j]
                gstdetails = getGstinfo(token, gst["gstin"])
                placeofbusiness = getPlaceOfBussinessinfo(token, gst["gstin"])
                if placeofbusiness is None:
                    sendAlert()
                    exit(0)
                if 'pradr' in placeofbusiness and 'adr' in placeofbusiness['pradr']:
                    gst["address"] = placeofbusiness['pradr']['adr']
                else:
                    gst["address"] = ""
                gst["taxpayertype"] = gstdetails['dty']
                newlist.append(gst)
            gstinobj["gstins"] = newlist
            finaldata.append(gstinobj)
            logging.info("New GST data for the PAN: " + str(gstinobj))

        for i in range(len(finaldata)):
            key_to_check = {"pan": finaldata[i]["pan"]}
            exiting_data = colletion.find_one(key_to_check)
            if exiting_data is None:
                colletion.insert_one(finaldata[i])
            else:
                result = colletion.update_one(
                    key_to_check,
                    {
                        "$set": {
                            "gstins": finaldata[i]["gstins"]
                        }
                    })

    except Exception as e:
        logging.info("Exception :" + str(e))
        sendAlert()
