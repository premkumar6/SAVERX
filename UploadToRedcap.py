import dropbox
import requests
from Meganconfig import redcap_api_token
from Meganconfig import dropbox_app_key
from Meganconfig import dropbox_app_secret
from Meganconfig import auth_url
from Meganconfig import stored_refresh_token
from dropbox.exceptions import AuthError
import redcap
import csv
import pandas as pd
import json

# Function to list files in a folder
def list_files_in_folder(folder_path,dbx):
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
    
#############################################################################################################
def main():
    #Access Dropbox Account
    d_app_key = dropbox_app_key
    d_app_secret = dropbox_app_secret
    refresh_token = stored_refresh_token

    dbx = dropbox.Dropbox(oauth2_refresh_token=refresh_token, app_key=d_app_key, app_secret=d_app_secret)
    # Get information about the current user's account
    account_info = dbx.users_get_current_account()

    # # Access various properties of the user's account
    account_id = account_info.account_id
    display_name = account_info.name.display_name
    email = account_info.email
    is_verified = account_info.email_verified

    # Print the user's account information
    #print(f"Account ID: {account_id}")
    print(f"Display Name: {display_name}")
    print(f"Email: {email}")
    print("You have successfully accessed the Dropbox Account")

    dbx._session.verify = True

    # Initialize the REDCap project
    redcap_api_url = 'https://redcap-p-a.umms.med.umich.edu/api/'
    redcap_api_key = redcap_api_token

    #Access Dropbox Readyfor Redcap Folder
    folder_path = '/AHRQ_R18_Project_SAVERx/ReadyForRedcap/'
    # Get the list of files in the folder
    current_files = list_files_in_folder(folder_path, dbx)
    #print(current_files)
    for nfile in current_files:
        print(nfile)
        name = nfile.split('.')[0]
        #read in data from nfile to Process with RxNorm
        dropbox_file_path = f"{folder_path}{nfile}"
        print(f"Dropbox File Path: {dropbox_file_path}")

        #Retrieve the CSV file's content without Downloading it from Dropbox
        metadata, f = dbx.files_download(dropbox_file_path)
        #Read the CSV Content and convert it into a list of dictionaries
        data_list = []
        csv_reader = csv.DictReader(f.content.decode().splitlines(), delimiter=',')
        for row in csv_reader:
            data_list.append(row)
        print(data_list[0:2])
        print("Data is Ready to Import to REDcap")

        fields = {
            'token': redcap_api_key,
            'content': 'metadata',
            'format': 'json'
        }
        r = requests.post(redcap_api_url,data=fields)
        print(f"HTTP Status: + {str(r.status_code)}")
        print(r.text)
         # Use PyCap's Project.import_file() to upload records from the CSV file
        for ind_record in data_list:
            data = json.dumps([ind_record])
            fields = {
                'token': redcap_api_key,
                'content': 'record',
                'format': 'json',
                'type': 'flat',
                'data': data
            }
            r = requests.post(redcap_api_url,data=fields)
            print(f"HTTP Status: + {str(r.status_code)}")
            print(r.text)



if __name__ == "__main__":
    main()