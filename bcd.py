import json
import requests
from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from state_mapping_details import state_code_details
load_dotenv()
import time
from state_mapping_details import state_short_name_details

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


def getBookingData():
    token = getBCDToken()
    finalresult = []
    if token is not None:
        limit = offsetlimit = 50
        offset = 0
        i = 1
        while True:
            url = "https://travel-data-api.bcdtravel.com/v1/trips"
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + str(token)
            }
            logging.info("Offset: " + str(offset))
            body = {
                "limit": limit,
                "offset": offset,
            }
            for j in range(1, 3, 1):
                response = requests.post(url, headers=headers, data=json.dumps(body))
                if response.status_code == 200:
                    data = response.json()
                    result = data['result']
                    logging.info("Length of result: " + str(len(result)))
                    if len(result) != 0:
                        finalresult.extend(result)
                    offset = (offsetlimit * i) + 1
                    remainingRecords = data["remainingRecords"]
                    i = i + 1
                    if remainingRecords == 0:
                        return finalresult
                    break
    return finalresult


def insertHotelDetails(hotelDetails):
    db = client['bcd_hotel_booking']
    hotel_details_collection = db['hotel_details']
    for i in range(len(hotelDetails)):
        if hotelDetails[i]["type"] == "Hotel":
            hotelDetailObj = {}
            hotelDetailObj["hotel_name"] = hotelDetails[i]["property"]["name"]
            hotelDetailObj["hotel_code"] = hotelDetails[i]["property"]["code"]
            hotelDetailObj["hotel_address"] = hotelDetails[i]["property"]["address"]
            hotelDetailObj["hotel_state_short_name"] = hotelDetails[i]["property"]["address"]["region"]["code"]
            state_details = state_short_name_details.get(hotelDetails[i]["property"]["address"]["region"]["code"])
            hotelDetailObj["hotel_state"] = state_details["state_name"]
            hotelDetailObj["hotel_state_code"] = state_details["state_code"]
            hotel_phone=[]
            if "phone" in hotelDetails[i]["property"] and "number" in hotelDetails[i]["property"]["phone"]:
                hotel_phone.append(hotelDetails[i]["property"]["phone"]["number"])
            hotelDetailObj["hotel_phone"]=hotel_phone
            key_to_check = {"hotel_code": hotelDetails[i]["property"]["code"]}
            exiting_data = hotel_details_collection.find_one(key_to_check)
            if exiting_data is None:
                hotel_details_collection.insert_one(hotelDetailObj)


def processData(booking_data):

    db = client['bcd_hotel_booking']
    client_details_collection = db['bcd_client_details']
    bcd_booking_details_collection = db['bcd_booking_details']
    pan_to_gstins=db['pan_to_gstins']

    customermap = {}
    insertedrecord = []
    updatedrecord = []
    pantogstinsmap={}

    bcd_client_details = list(client_details_collection.find())
    for data in bcd_client_details:
        address = ""
        if "name" in data:
            address += data["name"]
        if "street" in data:
            if address != "":
                address += ", "
            address += data["street"]
        if "address_information" in data:
            if address != "":
                address += ", "
            address += data["address_information"]
        if "state" in data:
            if address != "":
                address += ", "
            address += data["state"]
        data["gstin_detail"] = {
            "company_name": data["name"],
            "pan": data["pan"],
            "address": address,
        }
        customermap[data["customer_code"]] = data

    pan_to_gstins_details=list(pan_to_gstins.find())
    for data in pan_to_gstins_details:
        gstinsdata={}
        for i in range(len(data["gstins"])):
            gstin=data["gstins"][i]["gstin"]
            state_code=data["gstins"][i]["stateCd"]
            gst_status=data["gstins"][i]["authStatus"]
            address=data["gstins"][i]["address"]
            state_details=state_code_details.get(state_code)
            state_name=state_details["state_name"]
            state_short_name=state_details["short_name"]
            if state_short_name in gstinsdata:
                gstinsdata[state_short_name].append({"gstin":gstin,"state":state_name,"state_code":state_code,"state_short_name":state_short_name,"address":address,"gst_status":gst_status})
            else:
                gstinsdata[state_short_name]=[{"gstin":gstin,"state":state_name,"state_code":state_code,"state_short_name":state_short_name,"address":address,"gst_status":gst_status}]
            if state_short_name=="UK":
                gstinsdata["UT"]=[{"gstin":gstin,"state":state_name,"state_code":state_code,"state_short_name":"UT","address":address,"gst_status":gst_status}]

        pantogstinsmap[data["pan"]]=gstinsdata
    logging.info(pantogstinsmap)
    logging.info(customermap)
    if len(customermap) != 0:
        for i in range(len(booking_data)):
            data = booking_data[i]
            if "segments" in data and len(data["segments"]) != 0:
                insertHotelDetails(data["segments"])
                if "identification" in data and "recordLocator" in data["identification"]:
                    key_to_check = {"recordLocator": data["identification"]["recordLocator"]}
                    exiting_data = bcd_booking_details_collection.find_one(key_to_check)
                    if exiting_data is None:
                        # New record
                        status = "PENDING"
                        if "tripDetails" in data and "tripStatus" in data["tripDetails"] and data["tripDetails"][
                            "tripStatus"] == "cancelled":
                            status = "CANCELLED"
                        tempdoc = {}
                        customer_code = data["identification"]["customerNumber"]
                        if customer_code in customermap:
                            customer_details = customermap[customer_code]
                            company_name = customer_details["gstin_detail"]["company_name"]
                            customer_pan=customer_details["gstin_detail"]["pan"]
                            customer_address=customer_details["gstin_detail"]["address"]

                            hotel_state_short_name = []
                            gstin_details=[]
                            for j in range(len(data["segments"])):
                                # if "property" in data["segments"][j] and "region" in data["segments"][j]["property"]:
                                    hotel_state_code=data["segments"][j]["property"]["address"]["region"]["code"]
                                    hotel_state_short_name.append(hotel_state_code)
                                    if customer_pan in pantogstinsmap:
                                        if hotel_state_code in pantogstinsmap[customer_pan] and pantogstinsmap[customer_pan][hotel_state_code]["gst_status"] == "Active":
                                            state_details=pantogstinsmap[customer_pan][hotel_state_code][0]
                                            state=state_details["state"]
                                            state_code=state_details["state_code"]
                                            state_short_name=state_details["state_short_name"]
                                            gstin=state_details["gstin"]
                                            address=company_name+", "+state_details["address"]
                                            gstin_details.append({
                                                "company_name":company_name,
                                                "pan": customer_pan,
                                                "gstin":gstin,
                                                "address":address,
                                                "state":state,
                                                "state_short_name":state_short_name,
                                                "state_code":state_code,
                                            })
                                        else:
                                            gstin_details.append({
                                                "company_name": company_name,
                                                "pan": customer_pan,
                                                "address": customer_address,
                                            })
                            tempdoc["workspace_id"] = customer_details["workspace_id"]
                            tempdoc["customer_code"] = customer_code
                            tempdoc["recordLocator"] = data["identification"]["recordLocator"]
                            if len(gstin_details) != 0:
                                tempdoc["gstin_detail"] = gstin_details
                            else:
                                tempdoc["gstin_detail"]=[{
                                    "company_name": company_name,
                                    "pan": customer_pan,
                                    "address": customer_address,
                                }]
                            tempdoc["status"] = status
                            tempdoc["booking_data"] = data
                            bcd_booking_details_collection.insert_one(tempdoc)
                            insertedrecord.append(tempdoc)
                        else:
                            status = "PENDING"
                            if "tripDetails" in data and "tripStatus" in data["tripDetails"] and data["tripDetails"][
                                "tripStatus"] == "cancelled":
                                status = "CANCELLED"
                            tempdoc["recordLocator"] = data["identification"]["recordLocator"]
                            tempdoc["status"] = status
                            tempdoc["booking_data"] = data
                            bcd_booking_details_collection.insert_one(tempdoc)
                            insertedrecord.append(tempdoc)
                    else:
                        status = exiting_data["status"]
                        if "tripDetails" in data and "tripStatus" in data["tripDetails"] and data["tripDetails"][
                            "tripStatus"] == "cancelled":
                            status = "CANCELLED"
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

    return insertedrecord, updatedrecord


if __name__ == '__main__':
    logging.info("===================================================")
    datedata = getDate()
    logging.info("Processing for date : " + str(datedata))
    booking_data = getBookingData()
    if booking_data is not None:
        logging.info("No. of booking data found: " + str(len(booking_data)))
        logging.info("Processing Booking Data")
        insertedrecord, updatedrecord = processData(booking_data)
        logging.info("Inserted Rows: " + str(len(insertedrecord)))
        logging.info("Updated Rows: " + str(len(updatedrecord)))
    logging.info("===================================================")
