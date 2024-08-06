from dotenv import load_dotenv
load_dotenv()
import logging
import os
import requests
from sendgrid.helpers.mail import Mail
from pymongo import MongoClient
from datetime import datetime,timedelta
import pytz
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)



SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
MONGO_URL = os.getenv('MONGO_URL')

# client = MongoClient(MONGO_URL, maxIdleTimeMS=None)

def get_mongo_client(retries=5, delay=5):
    for attempt in range(retries):
        try:
            client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
            client.admin.command('ping')
            logging.info("Connected to MongoDB")
            return client
        except Exception as e:
            logging.info(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
client = get_mongo_client()
def send_email(to_emails, template_id, dynamic_template_data):
    api_key = SENDGRID_API_KEY
    url = 'https://api.sendgrid.com/v3/mail/send'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}',
    }

    for to_email in to_emails:
        message = Mail(
            from_email='alerts@finkraft.ai',
            to_emails=to_email,
        )
        message.template_id = template_id
        message.dynamic_template_data = dynamic_template_data

        try:
            # Convert the Mail object to JSON
            response = requests.post(
                url,
                headers=headers,
                json=message.get(),
                verify=False  # Disable SSL verification
            )
            logging.info(f"Email sent to {to_email} successfully! Status code: {response.status_code}")
        except Exception as e:
            logging.info(f"Error sending email to {to_email}: {e}")


def getDetails():
    dynamic_template_data = {
        'currentDate': None,
        'total_new_booking': 0,
        'total_hotel_missing_emails': 0,
        'total_hotel_missing_gstin': 0,
        'total_deferred_emails': 0,
        'total_bounced_emails': 0,
        'total_spammed_emails': 0,
        'total_unsub_emails': 0,
        'total_dropped_emails': 0,
        'button_link': 'https://hotel.finkraftai.com/hotel_booking'
    }
    ist = pytz.timezone('Asia/Kolkata')
    # Get the current time in IST
    current_time_ist = datetime.now(ist)
    current_time_ist=current_time_ist.strftime('%d-%m-%Y')
    dynamic_template_data['currentDate'] = current_time_ist

    bcd_booking_db = client["bcd_hotel_booking"]

    bcd_booking_details_collection = bcd_booking_db['bcd_booking_details']
    total_new_booking = bcd_booking_details_collection.count_documents({"status": "PENDING"})
    dynamic_template_data['total_new_booking'] = total_new_booking

    hotel_details_collection=bcd_booking_db["hotel_details"]
    total_hotel_missing_emails = hotel_details_collection.count_documents({"hotel_emails": {"$exists": False}})
    dynamic_template_data['total_hotel_missing_emails'] = total_hotel_missing_emails

    total_hotel_missing_gstin = hotel_details_collection.count_documents({"hotel_gstin": {"$exists": False}})
    dynamic_template_data['total_hotel_missing_gstin'] = total_hotel_missing_gstin

    email_scheduler_db = client['email_scheduler']
    email_events_collection = email_scheduler_db['email_events']
    now = datetime.now()
    yesterday_midnight = datetime(now.year, now.month, now.day) - timedelta(days=1)
    today_midnight = datetime(now.year, now.month, now.day)

    start_timestamp = int(yesterday_midnight.timestamp())
    end_timestamp = int(today_midnight.timestamp())
    logging.info("StartTime: "+str(start_timestamp))
    logging.info("EndTime: "+str(end_timestamp))
    pipeline = [
        {
            "$match": {
                "timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}
            }
        },
        {
            "$group": {
                "_id": "$event",
                "count": {"$sum": 1}
            }
        }
    ]

    results = email_events_collection.aggregate(pipeline)
    for result in results:
        if result['_id'] == "deferred":
            dynamic_template_data["total_deferred_emails"]=result['count']
        if result['_id'] == "bounce":
            dynamic_template_data["total_bounced_emails"] = result['count']
        if result['_id'] == "spamreport":
            dynamic_template_data["total_spammed_emails"] = result['count']
        if result['_id'] == "unsubscribe":
            dynamic_template_data["total_unsub_emails"] = result['count']
        if result['_id'] == "dropped":
            dynamic_template_data["total_dropped_emails"] = result['count']

    return dynamic_template_data




if __name__ == "__main__":
    dynamic_template_data=getDetails()
    to_emails = [
        "komalkant@kgrp.in",
        "tabrez@kgrp.in",
        "indrani@kgrp.in",
        "rakesh@kgrp.in"
    ]
    template_id = 'd-6fce724139e44c02b79dbcd5c4482e3e'
    send_email(to_emails, template_id, dynamic_template_data)
    logging.info("Mail sent")

