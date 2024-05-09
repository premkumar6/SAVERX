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

KNACK_APPLICATION_ID = '6632dc762e8b5900292eba41'
KNACK_API_KEY = '380a40ea-65c1-487a-bc3b-43edcde4d6ea'
KNACK_OBJECT_KEY = 'object_4'
KNACK_API_URL = f'https://api.knack.com/v1/objects/object_4/records'

knack_headers = {
    'X-Knack-Application-ID': KNACK_APPLICATION_ID,
    'X-Knack-REST-API-Key': KNACK_API_KEY,
    'Content-Type': 'application/json'
}

# Function to list files in a folder
def list_files_in_folder(folder_path, dbx):
    """
        Creates a list of the files found in the folder on Dropbox.
    Parameters:
        folder_path (string): The path to the folder
        dbx: enables dropbox access
    Returns:
        files (list): List of Files in the folder
    """
    try:
        result = dbx.files_list_folder(folder_path)
        files = [entry.name for entry in result.entries if isinstance(entry, dropbox.files.FileMetadata)]
        return files
    except dropbox.exceptions.ApiError as e:
        print(f'Error listing folder contents: {e}')
        return []

def get_csv_content_from_dropbox(file_path, dbx):
    metadata, f = dbx.files_download(file_path)
    return f.content.decode()

def upload_data_to_knack(data_list):
    for record in data_list:
        # knack_payload = {
        #     "record_id": record["record_id"],
        #     "report_id": record["report_id"],
        #     "redcap_data_access_group": record["redcap_data_access_group"],
        #     "report_row_number": record["report_row_number"],
        #     "match_status": record["match_status"],
        #     "incorrect_action___1": record["incorrect_action___1"],
        #     "incorrect_action___2": record["incorrect_action___2"],
        #     "incorrect_action___3": record["incorrect_action___3"],
        #     "incorrect_action___4": record["incorrect_action___4"],
        #     "erx_ndc": record["erx_ndc"],
        #     "erx_ingredient": record["erx_ingredient"],
        #     "erx_dose_form": record["erx_dose_form"],
        #     "erx_strength": record["erx_strength"],
        #     "medication_prescribed": record["medication_prescribed"],
        #     "medication_dispensed": record["medication_dispensed"],
        #     "pharm_ndc": record["pharm_ndc"],
        #     "pharm_ingredient": record["pharm_ingredient"],
        #     "pharm_dose_form": record["pharm_dose_form"],
        #     "pharm_strength": record["pharm_strength"],
        # }
        # print(json.dumps(knack_payload))
        # print(knack_payload["record_id"])
        # break
        knack_payload = {
            "field_9": record["record_id"],
            "field_10": record["report_id"],
            "field_11": record["redcap_data_access_group"],
            "field_12": record["report_row_number"],
            "field_13": record["match_status"],
            "field_14": record["incorrect_action___1"],
            "field_15": record["incorrect_action___2"],
            "field_16": record["incorrect_action___3"],
            "field_17": record["incorrect_action___4"],
            "field_18": record["erx_ndc"],
            "field_19": record["erx_ingredient"],
            "field_20": record["erx_dose_form"],
            "field_21": record["erx_strength"],
            "field_22": record["medication_prescribed"],
            "field_23": record["medication_dispensed"],
            "field_24": record["pharm_ndc"],
            "field_25": record["pharm_ingredient"],
            "field_26": record["pharm_dose_form"],
            "field_27": record["pharm_strength"],
            # "field_27": record["page_number']
        }
        # print(f"Knack payload: {knack_payload}")
        # break
        json_data = (json.dumps([knack_payload]))[1:-1]
        print((json_data))
        # break

        response = requests.post(
            KNACK_API_URL,
            headers=knack_headers,
            data=json_data
        )
        print(f"Response content: {response.content}")
        if response.status_code == 200:
            print(f"Record uploaded successfully: {response.json()}")
        else:
            print(f"Error uploading data to Knack: {response.status_code} {response.text}")


# def upload_data_to_knack(data_list):
#     for record in data_list:
#         knack_payload = {knack_field_mappings[col]: record[col]
#                          for col in record if col in knack_field_mappings}
#         json_payload = json.dumps(knack_payload)
#         response = requests.post(KNACK_API_URL, headers=knack_headers, data=json_payload)
#         if response.status_code != 200:
#             print(f'Error uploading data to Knack: {response.text}')

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
        #print(data_list)
        # Upload data to Knack
        upload_data_to_knack(data_list)


def main():
    process_dropbox_files_to_knack()

if __name__ == "__main__":
    main()