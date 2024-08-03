import json
import requests
from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv
load_dotenv()
from state_mapping_details import state_code_details
from state_mapping_details import state_short_name_details

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

MONGO_URL = os.getenv('MONGO_URL')
client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")
def getNonCompletedData():
    db = client['bcd_hotel_booking']
    client_details_collection = db['bcd_client_details']
    bcd_booking_details_collection = db['bcd_booking_details_test']
    pan_to_gstins = db['pan_to_gstins']

    customermap = {}
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
        for j in range(len(data["gstins"])):
            gstin = data["gstins"][j]["gstin"]
            state_code = data["gstins"][j]["stateCd"]
            gst_status = data["gstins"][j]["authStatus"]
            address = data["gstins"][j]["address"]
            state_details = state_code_details.get(state_code)
            state_name = state_details["state_name"]
            state_short_name = state_details["short_name"]
            taxpayertype = data["gstins"][j]["taxpayertype"]
            if gst_status == "Active" and taxpayertype == "Regular":
                if state_short_name in gstinsdata:
                    gstinsdata[state_short_name].append({"gstin": gstin, "state": state_name, "state_code": state_code,
                                                         "state_short_name": state_short_name, "address": address,
                                                         "gst_status": gst_status, "taxpayertype": taxpayertype})
                else:
                    gstinsdata[state_short_name] = [{"gstin": gstin, "state": state_name, "state_code": state_code,
                                                     "state_short_name": state_short_name, "address": address,
                                                     "gst_status": gst_status, "taxpayertype": taxpayertype}]
                if state_short_name == "UK":
                    gstinsdata["UT"] = [
                        {"gstin": gstin, "state": state_name, "state_code": state_code, "state_short_name": "UT",
                         "address": address, "gst_status": gst_status, "taxpayertype": taxpayertype}]
            elif gst_status == "Inactive":
                if state_short_name in gstinsdata:
                    gstinsdata[state_short_name].append({"gst_status": gst_status})
                else:
                    gstinsdata[state_short_name] = [{"gst_status": gst_status}]
                if state_short_name == "UK":
                    gstinsdata["UT"] = [{"gst_status": gst_status}]

            elif gst_status == "Active" and taxpayertype == "Input Service Distributor (ISD)":
                if state_short_name in gstinsdata:
                    gstinsdata[state_short_name].append({"gst_status": taxpayertype})
                else:
                    gstinsdata[state_short_name] = [{"gst_status": taxpayertype}]
                if state_short_name == "UK":
                    gstinsdata["UT"] = [{"gst_status": taxpayertype}]

        pantogstinsmap[data["pan"]] = gstinsdata

    logging.info(pantogstinsmap)
    logging.info(customermap)
    logging.info("=============================================================================")
    booking_data=list(bcd_booking_details_collection.find({ "status": { "$ne": "COMPLETED" } }))
    if len(customermap) != 0:
        for i in range(len(booking_data)):
            data = booking_data[i]["booking_data"]
            logging.info("Old GST Info for recordLocator: "+str(data["identification"]["recordLocator"]) + " Data: "+str(booking_data[i]["gstin_detail"]))
            key_to_check = {"recordLocator": data["identification"]["recordLocator"]}
            exiting_data = bcd_booking_details_collection.find_one(key_to_check)
            if exiting_data is not None:
                customer_code = data["identification"]["customerNumber"]
                tempdoc = {}
                gstin_details = []
                if customer_code in customermap:
                    customer_details = customermap[customer_code]
                    company_name = customer_details["gstin_detail"]["company_name"]
                    customer_pan = customer_details["gstin_detail"]["pan"]
                    customer_address = customer_details["gstin_detail"]["address"]
                    workspace_id = customer_details["workspace_id"]

                    for j in range(len(data["segments"])):
                        if "property" in data["segments"][j] and "address" in data["segments"][j][
                            "property"] and "region" in data["segments"][j]["property"]["address"] and "code" in \
                                data["segments"][j]["property"]["address"]["region"]:
                            hotel_state_code = data["segments"][j]["property"]["address"]["region"]["code"]
                            if customer_pan in pantogstinsmap:
                                if hotel_state_code in pantogstinsmap[customer_pan]:
                                    active = None
                                    isd = None
                                    inactive = None
                                    for k in range(len(pantogstinsmap[customer_pan][hotel_state_code])):
                                        if pantogstinsmap[customer_pan][hotel_state_code][k]["gst_status"] == "Active":
                                            active = k
                                            break
                                        if pantogstinsmap[customer_pan][hotel_state_code][k][
                                            "gst_status"] == "Input Service Distributor (ISD)":
                                            isd = k
                                        if pantogstinsmap[customer_pan][hotel_state_code][k][
                                            "gst_status"] == "Inactive":
                                            inactive = k

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

                                # Need to test
                                else:
                                    gstin_details.append({
                                        "company_name": company_name,
                                        "pan": customer_pan,
                                        "address": customer_address,
                                        "remark": "Customer GSTIN details is missing for the state code " + str(
                                            hotel_state_code)
                                    })

                            # Need to test
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
                    # Need  to test
                else:
                    if customer_code is not None:
                        gstin_details.append({
                            "remark": "Customer details is missing for customer code " + str(customer_code)
                        })

                if len(gstin_details) != 0:
                    tempdoc["gstin_detail"] = gstin_details
                    logging.info("New GST Info for recordLocator: " + str(
                        data["identification"]["recordLocator"]) + " Data: " + str(tempdoc["gstin_detail"]))
                    result = bcd_booking_details_collection.update_one(
                        key_to_check,
                        {
                            "$set": {
                                "gstin_detail": tempdoc["gstin_detail"]
                            }
                        })
                    logging.info("=============================================================================")


if __name__ == '__main__':
    getNonCompletedData()

