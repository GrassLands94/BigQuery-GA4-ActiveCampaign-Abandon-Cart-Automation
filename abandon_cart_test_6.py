import os
import logging
from google.cloud import bigquery
import requests
import json
import time


# Configure logging
logging.basicConfig(level=logging.INFO)

# Set the path to the credentials JSON file for google big query
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/edohner/Documents/python_test/loc-bigquery-8b407ce7ea63.json"

# Load the configuration file for active campaign
with open("C:/Users/edohner/Documents/python_test/activecampaign_api_config.json") as config_file:
    config = json.load(config_file)

# Constants
API_URL = 'https://lyricoperaofchicago.api-us1.com/api/3'
API_KEY = config.get('active_campaign_api_key')
patron_id_custom_field_id = 4
abandoned_cart_product_quantity_price_custom_field_id = 823
abandoned_cart_tag_id = 6443

# Initialize BigQuery client
client = bigquery.Client()

# BigQuery Query
query = """
    SELECT user_id, product_quantity_price
    FROM loc-bigquery.analytics_314964580.abandoned_cart_user_ids
"""

# Run the query
query_job = client.query(query)
if query_job:
    logging.info('Successfully retrieved data from BigQuery')
else:
    logging.error('Failed to retrieve data from BigQuery')      

# Initialize the lists for user_id and product_quantity_price
patron_id_custom_field_values = []
abandoned_cart_product_quantity_price_custom_field_values = []

# append data to lists
for row in query_job:
    patron_id_custom_field_values.append(row['user_id'])  
    abandoned_cart_product_quantity_price_custom_field_values.append(row['product_quantity_price'])  

print(patron_id_custom_field_values)
print(abandoned_cart_product_quantity_price_custom_field_values)

# Headers for authentication
headers = {
    'Api-Token': API_KEY,
    'Content-Type': 'application/json',
}




def active_campaign_contact_id(patron_id_custom_field_id, patron_id_custom_field_value):
    """Find ActiveCampaign contact_id by custom field value."""
    # Constants for API throttling
    wait_time = 60  # Initial wait time (60 seconds)
    max_wait_time = 3600  # Maximum wait time (1 hour)
    max_attempts = 10  # Maximum number of attempts to make

    # Variable for API throttling
    attempts = 0  # To track the number of attempts

    while attempts < max_attempts:
        response = requests.get(
            f'{API_URL}/fieldValues?filters[fieldid]={patron_id_custom_field_id}&filters[val]={patron_id_custom_field_value}', 
            headers=headers
        )

        if response.status_code == 200:
            field_values = response.json().get('fieldValues', [])
            if field_values:
                # Contact found: exit the loop and return the contact ID
                return field_values[0].get('contact')
            else:
                # No contact found: log the message and exit the loop
                logging.info(f"No contact found for field ID: {patron_id_custom_field_id} and value: {patron_id_custom_field_value}")
                return None  # No retries, just return None
        elif response.status_code == 429:
            # Rate limit exceeded: wait and retry
            logging.warning(f'Rate limit exceeded (429). Waiting {wait_time} seconds before retrying...')
            time.sleep(wait_time)
            attempts += 1

            # Increase wait time with each retry, but don't exceed max wait time
            wait_time = min(wait_time * 2, max_wait_time)
        else:
            # Any other error: log and break
            logging.error(f"API call failed with status code: {response.status_code} - {response.text}")
            return None  # Exit without retrying

    # Only if maximum retries are hit due to rate limiting or issues
    if attempts >= max_attempts:
        logging.error("Maximum attempts reached due to API throttling. Exiting function: active_campaign_contact_id()")
    
    return None



def add_tag_to_contact(contact_id, tag_id):
    """Add a tag to a contact in ActiveCampaign."""

    #constants for api throttling
    wait_time = 60  # Initial wait time (60 seconds)
    max_wait_time = 3600  # Maximum wait time (1 hour)
    max_attempts = 10  # Maximum number of attempts to make

    #variable for api throttling
    attempts = 0  # To track number of attempts

    data = {"contactTag": {"contact": contact_id, "tag": tag_id}}

    while attempts < max_attempts:
        response = requests.post(f'{API_URL}/contactTags', json=data, headers=headers)

        if response.status_code in (200, 201):
            logging.info(f'Tag {tag_id} added for contact_id: {contact_id}')
            return None
        elif response.status_code == 429:
            logging.warning(f'Rate limit exceeded (429). Waiting {wait_time} seconds before retrying...')
            time.sleep(wait_time)
            attempts += 1

            # Increase wait time with each retry, but don't exceed max wait time
            wait_time = min(wait_time * 2, max_wait_time)
        else:
            logging.error(f"Failed to add tag. Status: {response.status_code}, Response: {response.text}")
            return None
    # Only if maximum retries are hit due to rate limiting or issues
    if attempts >= max_attempts:
        logging.error("Maximum attempts reached due to API throttling. Exiting function: active_campaign_contact_id()")
        return None

def post_cart_data(contact_id, abandoned_cart_product_quantity_price_custom_field_id, abandoned_cart_product_quantity_price_custom_field_value):
    """Post data to a custom field for the contact."""

    #constants for api throttling
    wait_time = 60  # Initial wait time (60 seconds)
    max_wait_time = 3600  # Maximum wait time (1 hour)
    max_attempts = 10  # Maximum number of attempts to make

    #variable for api throttling
    attempts = 0  # To track number of attempts 


    data = {
        "fieldValue": {
            "contact": contact_id,
            "field": abandoned_cart_product_quantity_price_custom_field_id,
            "value": abandoned_cart_product_quantity_price_custom_field_value
        }
    }
    while attempts < max_attempts:

        response = requests.post(f'{API_URL}/fieldValues', json=data, headers=headers)

        if response.status_code in (200, 201):
            logging.info(f'Successfully created/updated field value for contact_id: {contact_id}')
            return None
        elif response.status_code == 429:
            logging.warning(f'Rate limited exceeded (429). Waiting {wait_time} seconds before retrying')
            time.sleep(wait_time)
            attempts +=1

            # Increase wait time with each retry, but don't exceed max wait time
            wait_time = min(wait_time * 2, max_wait_time)
        else:
            logging.error(f'Failed to create field value. Status: {response.status_code}, Response: {response.text}')
            return None
    if attempts >= max_attempts:
        logging.error(f'Maximum attempts reached. Exiting function: post_cart_data')
        return None

  
def tag_and_add_cart_data(patron_id_custom_field_id, patron_id_custom_field_values, tag_id, abandoned_cart_product_quantity_price_custom_field_id, abandoned_cart_product_quantity_price_custom_field_values):
    """Tag abandoned cart users and add cart data."""
    for user_id, product_quantity_value_string in zip(patron_id_custom_field_values, abandoned_cart_product_quantity_price_custom_field_values):
        contact_id = active_campaign_contact_id(patron_id_custom_field_id, user_id)

        if not contact_id:
            
            continue

        add_tag_to_contact(contact_id, tag_id)
        post_cart_data(contact_id, abandoned_cart_product_quantity_price_custom_field_id, product_quantity_value_string)

# Run the tagging and data posting
tag_and_add_cart_data(patron_id_custom_field_id, patron_id_custom_field_values, abandoned_cart_tag_id, abandoned_cart_product_quantity_price_custom_field_id, abandoned_cart_product_quantity_price_custom_field_values)