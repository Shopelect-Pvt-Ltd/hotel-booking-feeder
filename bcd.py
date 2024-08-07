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
from datetime import datetime
import pytz

# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

MONGO_URL = os.getenv('MONGO_URL')
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
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


def sendMail(message):
    # Define the timezone for IST
    ist = pytz.timezone('Asia/Kolkata')
    # Get the current time in IST
    current_time_ist = datetime.now(ist)
    sender_email = "alerts@finkraft.ai"
    recipient_email = ["komalkant@kgrp.in"]
    subject = "Exception happened in the BCD" + str(current_time_ist.strftime('%Y-%m-%d %H:%M:%S'))
    content = "Exception happened in the BCD hotel booking details please fix it.Message: " + str(message)

    url = "https://api.sendgrid.com/v3/mail/send"
    headers = {
        "Authorization": f"Bearer {SENDGRID_API_KEY}",
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
    logging.info("Hotel Details: " + str(hotelDetails))
    db = client['bcd_hotel_booking']
    hotel_details_collection = db['hotel_details']
    for i in range(len(hotelDetails)):
        if hotelDetails[i]["type"] == "Hotel":
            hotelDetailObj = {}
            hotelDetailObj["hotel_name"] = hotelDetails[i]["property"]["name"]
            hotelDetailObj["hotel_code"] = hotelDetails[i]["property"]["code"]
            hotelDetailObj["hotel_address"] = hotelDetails[i]["property"]["address"]
            logging.info(hotelDetails)
            if "property" in hotelDetails[i] and "address" in hotelDetails[i]["property"] and "region" in \
                    hotelDetails[i]["property"]["address"] and "code" in hotelDetails[i]["property"]["address"][
                "region"]:
                hotelDetailObj["hotel_state_short_name"] = hotelDetails[i]["property"]["address"]["region"]["code"]
                state_details = state_short_name_details.get(hotelDetails[i]["property"]["address"]["region"]["code"])
                hotelDetailObj["hotel_state"] = state_details["state_name"]
                hotelDetailObj["hotel_state_code"] = state_details["state_code"]
            hotel_phone = []
            if "phone" in hotelDetails[i]["property"] and "number" in hotelDetails[i]["property"]["phone"]:
                hotel_phone.append(hotelDetails[i]["property"]["phone"]["number"])
            hotelDetailObj["hotel_phone"] = hotel_phone
            key_to_check = {"hotel_code": hotelDetails[i]["property"]["code"]}
            existing_data = hotel_details_collection.find_one(key_to_check)
            if existing_data is None:
                hotel_details_collection.insert_one(hotelDetailObj)


def insertBookingUpdateLogs(existing_data):
    db = client['bcd_hotel_booking']
    bcd_booking_update_logs_collection = db['bcd_booking_update_logs']
    del existing_data['_id']
    bcd_booking_update_logs_collection.insert_one(existing_data)


def getGstinDetails(data, customermap, pantogstinsmap):
    gstin_details = []
    workspace_id = None
    customer_code = None
    if "identification" in data and "customerNumber" in data["identification"]:
        customer_code = data["identification"]["customerNumber"]

    if customer_code in customermap:
        customer_details = customermap[customer_code]
        company_name = customer_details["gstin_detail"]["company_name"]
        customer_pan = customer_details["gstin_detail"]["pan"]
        customer_address = customer_details["gstin_detail"]["address"]
        workspace_id = customer_details["workspace_id"]
        for i in range(len(data["segments"])):
            if "property" in data["segments"][i] and "address" in data["segments"][i]["property"] and "region" in \
                    data["segments"][i]["property"]["address"] and "code" in data["segments"][i]["property"]["address"][
                "region"]:
                hotel_state_code = data["segments"][i]["property"]["address"]["region"]["code"]
                if customer_pan in pantogstinsmap:
                    if hotel_state_code in pantogstinsmap[customer_pan]:
                        active = None
                        isd = None
                        inactive = None
                        for j in range(len(pantogstinsmap[customer_pan][hotel_state_code])):
                            if pantogstinsmap[customer_pan][hotel_state_code][j]["gst_status"] == "Active":
                                active = j
                                break
                            if pantogstinsmap[customer_pan][hotel_state_code][j][
                                "gst_status"] == "Input Service Distributor (ISD)":
                                isd = j
                            if pantogstinsmap[customer_pan][hotel_state_code][j]["gst_status"] == "Inactive":
                                inactive = j

                        if active is not None:
                            state_details = pantogstinsmap[customer_pan][hotel_state_code][active]
                            state = state_details["state"]
                            state_code = state_details["state_code"]
                            state_short_name = state_details["state_short_name"]
                            gstin = state_details["gstin"]
                            address = company_name + ", " + state_details["address"]
                            gstin_details.append({
                                "company_name": company_name,
                                "pan": customer_pan,
                                "gstin": gstin,
                                "address": address,
                                "state": state,
                                "state_short_name": state_short_name,
                                "state_code": state_code,
                                "remark": "Active"
                            })

                        elif isd is not None:
                            gstin_details.append({
                                "company_name": company_name,
                                "pan": customer_pan,
                                "address": customer_address,
                                "remark": "Input Service Distributor (ISD)"
                            })

                        elif inactive is not None:
                            gstin_details.append({
                                "company_name": company_name,
                                "pan": customer_pan,
                                "address": customer_address,
                                "remark": "Inactive"
                            })

                    else:
                        gstin_details.append({
                            "company_name": company_name,
                            "pan": customer_pan,
                            "address": customer_address,
                            "remark": "Customer GSTIN details is missing for the state code " + str(hotel_state_code)
                        })

                else:
                    gstin_details.append({
                        "company_name": company_name,
                        "pan": customer_pan,
                        "address": customer_address,
                        "remark": "Customer GSTIN details is missing for the PAN " + str(customer_pan)
                    })
            else:
                gstin_details.append({
                    "company_name": company_name,
                    "pan": customer_pan,
                    "address": customer_address,
                    "remark": "Hotel region code is missing in booking data"
                })
    else:
        if customer_code is not None:
            gstin_details.append({
                "remark": "Customer details is missing for customer code " + str(customer_code)
            })

    return gstin_details, workspace_id, customer_code


def processData(booking_data):
    db = client['bcd_hotel_booking']
    client_details_collection = db['bcd_client_details']
    bcd_booking_details_collection = db['bcd_booking_details']
    pan_to_gstins = db['pan_to_gstins']

    customermap = {}
    insertedrecord = []
    updatedrecord = []
    pantogstinsmap = {}

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

    pan_to_gstins_details = list(pan_to_gstins.find())
    for data in pan_to_gstins_details:
        gstinsdata = {}
        for i in range(len(data["gstins"])):
            gstin = data["gstins"][i]["gstin"]
            state_code = data["gstins"][i]["stateCd"]
            gst_status = data["gstins"][i]["authStatus"]
            address = data["gstins"][i]["address"]
            state_details = state_code_details.get(state_code)
            state_name = state_details["state_name"]
            state_short_name = state_details["short_name"]
            taxpayertype = data["gstins"][i]["taxpayertype"]
            if gst_status == "Active" and taxpayertype == "Regular":
                if state_short_name in gstinsdata:
                    gstinsdata[state_short_name].append({"gstin": gstin, "state": state_name, "state_code": state_code,
                                                         "state_short_name": state_short_name, "address": address,
                                                         "gst_status": gst_status, "taxpayertype": taxpayertype})
                else:
                    gstinsdata[state_short_name] = [{"gstin": gstin, "state": state_name, "state_code": state_code,
                                                     "state_short_name": state_short_name, "address": address,
                                                     "gst_status": gst_status, "taxpayertype": taxpayertype}]
                if state_short_name == "UT":
                    gstinsdata["UK"] = [
                        {"gstin": gstin, "state": state_name, "state_code": state_code, "state_short_name": "UK",
                         "address": address, "gst_status": gst_status, "taxpayertype": taxpayertype}]
                if state_short_name == "CG":
                    gstinsdata["CT"] = [
                        {"gstin": gstin, "state": state_name, "state_code": state_code, "state_short_name": "CT",
                         "address": address, "gst_status": gst_status, "taxpayertype": taxpayertype}]


            elif gst_status == "Inactive":
                if state_short_name in gstinsdata:
                    gstinsdata[state_short_name].append({"gst_status": gst_status})
                else:
                    gstinsdata[state_short_name] = [{"gst_status": gst_status}]
                if state_short_name == "UT":
                    gstinsdata["UK"] = [{"gst_status": gst_status}]
                if state_short_name == "CG":
                    gstinsdata["CT"] = [{"gst_status": gst_status}]

            elif gst_status == "Active" and taxpayertype == "Input Service Distributor (ISD)":
                if state_short_name in gstinsdata:
                    gstinsdata[state_short_name].append({"gst_status": taxpayertype})
                else:
                    gstinsdata[state_short_name] = [{"gst_status": taxpayertype}]
                if state_short_name == "UT":
                    gstinsdata["UK"] = [{"gst_status": taxpayertype}]
                if state_short_name == "CG":
                    gstinsdata["CT"] = [{"gst_status": taxpayertype}]

        pantogstinsmap[data["pan"]] = gstinsdata

    logging.info(pantogstinsmap)
    logging.info(customermap)

    if len(customermap) != 0:
        for i in range(len(booking_data)):
            data = booking_data[i]
            if "segments" in data and len(data["segments"]) != 0:
                logging.info("Booking Data: " + str(data))
                insertHotelDetails(data["segments"])
                if "identification" in data and "recordLocator" in data["identification"]:
                    key_to_check = {"recordLocator": data["identification"]["recordLocator"]}
                    existing_data = bcd_booking_details_collection.find_one(key_to_check)
                    if existing_data is None:
                        # New record
                        status = "PENDING"
                        if "tripDetails" in data and "tripStatus" in data["tripDetails"] and data["tripDetails"][
                            "tripStatus"] == "cancelled":
                            status = "CANCELLED"

                        tempdoc = {}
                        gstin_details, workspace_id, customer_code = getGstinDetails(data, customermap, pantogstinsmap)
                        if len(gstin_details) != 0 and workspace_id is not None and customer_code is not None:
                            tempdoc["workspace_id"] = workspace_id
                            tempdoc["customer_code"] = customer_code
                            tempdoc["gstin_detail"] = gstin_details
                            tempdoc["recordLocator"] = data["identification"]["recordLocator"]
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
                        status = existing_data["status"]

                        if status == "COMPLETED":
                            continue

                        if "tripDetails" in data and "tripStatus" in data["tripDetails"] and data["tripDetails"][
                            "tripStatus"] == "cancelled":
                            status = "CANCELLED"

                        currLastModifiedDateTime = None
                        if "tripDetails" in data and "lastModifiedDateTime" in data["tripDetails"]:
                            currLastModifiedDateTime = data["tripDetails"]["lastModifiedDateTime"]

                        existingLastModifiedDateTime = None
                        if "booking_data" in existing_data and "tripDetails" in existing_data[
                            "booking_data"] and "lastModifiedDateTime" in existing_data["booking_data"]["tripDetails"]:
                            existingLastModifiedDateTime = existing_data["booking_data"]["tripDetails"][
                                "lastModifiedDateTime"]

                        if currLastModifiedDateTime is not None and existingLastModifiedDateTime is not None and currLastModifiedDateTime != existingLastModifiedDateTime:
                            insertBookingUpdateLogs(existing_data)

                        gstin_details, workspace_id, customer_code = getGstinDetails(data, customermap, pantogstinsmap)
                        if len(gstin_details) != 0:
                            result = bcd_booking_details_collection.update_one(
                                key_to_check,
                                {
                                    "$set": {
                                        "status": status,
                                        "gstin_detail": gstin_details,
                                        "booking_data": data
                                    }
                                })

                            if result.matched_count > 0:
                                logging.info("Updated the document for " + str(key_to_check))
                                existing_data["status"] = status
                                existing_data["booking_data"] = data
                                updatedrecord.append(existing_data)
                            else:
                                logging.info("No updates for the bookingId: " + str(key_to_check))

    return insertedrecord, updatedrecord


if __name__ == '__main__':
    try:
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
    except Exception as e:
        logging.info(str(e))
        sendMail(str(e))
