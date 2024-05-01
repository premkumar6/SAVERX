import dropbox
import requests
import csv
from io import StringIO
from Meganconfig import dropbox_app_key
from Meganconfig import dropbox_app_secret
from Meganconfig import stored_refresh_token
from dropbox.exceptions import AuthError
import json

# Replace these with your actual secrets and API information
DROPBOX_APP_KEY = dropbox_app_key
DROPBOX_APP_SECRET = dropbox_app_secret
DROPBOX_REFRESH_TOKEN = stored_refresh_token

KNACK_APPLICATION_ID = '663283a5db75550028baf098'
KNACK_API_KEY = '2e10ba6b-0235-4616-a893-58507e64f548'
KNACK_OBJECT_ID = 'redcap'
KNACK_API_URL = f'https://api.knack.com/v1/objects/redcap/records'

knack_field_mappings = {
    'record_id': 'knack_field_key_for_record_id',
    'report_id': 'knack_field_key_for_report_id',
    'redcap_data_access_group': 'knack_field_key_for_redcap_data_access_group',
    'report_row_number': 'knack_field_key_for_report_row_number',
    'match_status': 'knack_field_key_for_match_status',
    # Add mappings for the checkbox fields based on how checkboxes are structured in your Knack app
    'incorrect_action___1': 'knack_field_key_for_incorrect_action_1',
    'incorrect_action___2': 'knack_field_key_for_incorrect_action_2',
    'incorrect_action___3': 'knack_field_key_for_incorrect_action_3',
    'incorrect_action___4': 'knack_field_key_for_incorrect_action_4',
    'erx_ndc': 'knack_field_key_for_erx_ndc',
    'erx_ingredient': 'knack_field_key_for_erx_ingredient',
    'erx_dose_form': 'knack_field_key_for_erx_dose_form',
    'erx_strength': 'knack_field_key_for_erx_strength',
    'medication_prescribed': 'knack_field_key_for_medication_prescribed',
    'medication_dispensed': 'knack_field_key_for_medication_dispensed',
    'med12_medication_name': 'knack_field_key_for_med12_medication_name',
    'med12_dose_form': 'knack_field_key_for_med12_dose_form',
    'med12_strength': 'knack_field_key_for_med12_strength',
    'pharm_ndc': 'knack_field_key_for_pharm_ndc',
    'pharm_ingredient': 'knack_field_key_for_pharm_ingredient',
    'pharm_dose_form': 'knack_field_key_for_pharm_dose_form',
    'pharm_strength': 'knack_field_key_for_pharm_strength',
    'med12_pharm_Dispensed_ingredient': 'knack_field_key_for_med12_pharm_Dispensed_ingredient',
    'med12_pharm_Dispensed_form': 'knack_field_key_for_med12_pharm_Dispensed_form',
    'med12_pharm_Dispensed_strength_Description': 'knack_field_key_for_med12_pharm_Dispensed_strength_Description'
}

knack_headers = {
    'X-Knack-Application-ID': KNACK_APPLICATION_ID,
    'X-Knack-REST-API-Key': KNACK_API_KEY,
    'Content-Type': 'application/json'
}

# Function to list files in a folder
def list_files_in_folder(folder_path, dbx):
    # Function body remains the same

def get_csv_content_from_dropbox(file_path, dbx):
    metadata, f = dbx.files_download(file_path)
    return f.content.decode()

def upload_data_to_knack(data_list):
    for record in data_list:
        knack_payload = {knack_field_mappings[col]: record[col]
                         for col in record if col in knack_field_mappings}
        json_payload = json.dumps(knack_payload)
        response = requests.post(KNACK_API_URL, headers=knack_headers, data=json_payload)
        if response.status_code != 200:
            print(f'Error uploading data to Knack: {response.text}')

def process_dropbox_files_to_knack():
    # Dropbbox access
    dbx = dropbox.Dropbox(
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
        app_key=DROPBOX_APP_KEY,
        app_secret=DROPBOX_APP_SECRET
    )

    # Folder in Dropbox where the files are located
    folder_path = '/AHRQ_R18_Project_SAVERx/ReadyForRedcap/'

    # Get list of files from Dropbox
    files = list_files_in_folder(folder_path, dbx)
    for file_name in files:
        print(f"Processing file: {file_name}")
        file_path = folder_path + file_name
        csv_content = get_csv_content_from_dropbox(file_path, dbx)
        csv_reader = csv.DictReader(StringIO(csv_content))
        # Convert CSV records to list of dicts
        data_list = list(csv_reader)
        # Upload data to Knack
        upload_data_to_knack(data_list)
def main():
    process_dropbox_files_to_knack()

if __name__ == "__main__":
    main()