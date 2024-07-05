import json
import requests
from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
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

def getDate():
    # Get today's date
    today = datetime.today()
    # Format today's date
    formatted_today = today.strftime("%Y-%m-%d")
    # Get yesterday's date
    yesterday = today - timedelta(days=1)
    # Format yesterday's date
    formatted_yesterday = yesterday.strftime("%Y-%m-%d")
    # Combine both dates in the desired format
    date_range = f"{formatted_yesterday} TO {formatted_today}"
    return date_range

def getBCDToken():
    url = "https://auth.travel-data-api.bcdtravel.com/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "client_id": "20a00fnvtc5lksu041p96icg2g",
        "client_secret": "155p5o0au22cbc6eusk6d8fvgnvejenle7fhkdga8ha1tseht3un",
        "grant_type": "client_credentials"
    }

    for i in range(1, 3, 1):
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["access_token"]
        time.sleep(1)
    return None
def getBookingData(datedata):
    token = getBCDToken()
    if token is not None:
        url = "https://travel-data-api.bcdtravel.com/v1/trips"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + str(token)
        }

        body = {
            "lastModifiedDateTime": datedata
        }
        for i in range(1, 3, 1):
            response = requests.post(url, headers=headers, data=json.dumps(body))
            if response.status_code == 200:
                return response.json()['result']
    return None


def processData(booking_data):
    db = client['bcd_hotel_booking']
    client_details_collection = db['bcd_client_details']
    bcd_booking_details_collection=db['bcd_booking_details']
    bcd_client_details=list(client_details_collection.find({"gstin": {"$exists": True}}))
    customermap={}
    insertedrecord = []
    updatedrecord = []
    for data in bcd_client_details:
        address=""
        if "name" in data:
            address+=data["name"]
        if "Street" in data:
            if address != "":
                address+=", "
            address+=data["Street"]
        if "address_information" in data:
            if address != "":
                address+=", "
            address += data["address_information"]
        if "state" in data:
            if address != "":
                address+=", "
            address += data["state"]
        data["gstin_detail"]={
                "company_name":data["name"],
                "gstin":data["gstin"],
                "pan":data["pan"],
                "address":address,
                "state":data["state"],
                "state_code":data["state_code"]
        }
        customermap[data["customer_code"]]=data

    if len(customermap)!=0:
        for i in range(len(booking_data)):
            data = booking_data[i]
            if "segments" in data and len(data["segments"])!=0:
                if "identification" in data and "recordLocator" in data["identification"]:
                    key_to_check = {"recordLocator": data["identification"]["recordLocator"]}
                    exiting_data = bcd_booking_details_collection.find_one(key_to_check)
                    if exiting_data is None:
                        #New record
                        status="PENDING"
                        if "tripDetails" in data  and "tripStatus" in data["tripDetails"] and  data["tripDetails"]["tripStatus"]=="cancelled":
                            status="CANCELLED"
                        tempdoc={}
                        customer_code=data["identification"]["customerNumber"]
                        if customer_code in customermap:
                            customer_details=customermap[customer_code]
                            tempdoc["workspace_id"]=customer_details["workspace_id"]
                            tempdoc["customer_code"]=customer_code
                            tempdoc["recordLocator"]=data["identification"]["recordLocator"]
                            tempdoc["gstin_detail"]=customer_details["gstin_detail"]
                            tempdoc["status"]=status
                            tempdoc["booking_data"]=data
                            bcd_booking_details_collection.insert_one(tempdoc)
                            insertedrecord.append(tempdoc)
                    else:
                        status=exiting_data["status"]
                        if "tripDetails" in data  and "tripStatus" in data["tripDetails"] and  data["tripDetails"]["tripStatus"]=="cancelled":
                            status="CANCELLED"
                        result = bcd_booking_details_collection.update_one(
                            key_to_check,
                            {
                                "$set": {
                                    "status": status,
                                    "booking_data": data
                                }
                            })

                        if result.matched_count > 0:

                            logging.info("Updated the document for " + str(key_to_check))
                            exiting_data["status"] = status
                            exiting_data["booking_data"] = data
                            updatedrecord.append(exiting_data)
                        else:
                            logging.info("No updates for the bookingId: " + str(key_to_check))

    return insertedrecord,updatedrecord

if __name__ == '__main__':
    logging.info("===================================================")
    datedata=getDate()
    logging.info("Processing for date range: " + str(datedata))
    booking_data=getBookingData(datedata)
    if booking_data is not None:
        logging.info("No. of booking data found: " + str(len(booking_data)))
        logging.info("Processing Booking Data")
        insertedrecord,updatedrecord=processData(booking_data)
        logging.info("Inserted Rows: "+str(len(insertedrecord)))
        logging.info("Updated Rows: "+str(len(updatedrecord)))
    logging.info("===================================================")


