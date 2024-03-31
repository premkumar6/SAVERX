import dropbox
import time
import requests
from config import redcap_api_token
from Meganconfig import dropbox_app_key
from Meganconfig import dropbox_app_secret
from Meganconfig import auth_url
from Meganconfig import stored_refresh_token
import CachingProcessSaveRx
import CleanExcelSaveRxFiles
from dropbox.exceptions import AuthError
import os
import csv
from io import StringIO

#Function to determine if a file is a .csv or a .xlsx
def get_file_type(file_path):
    _, file_extension = os.path.splitext(file_path)
    if file_extension == '.csv':
        return 'csv'
    elif file_extension == '.xlsx':
        return 'xlsx'
    else:
        return 'unknown'  # Handle other file types or extensions

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

def main():

    ##This first step is done the first time only!
    ##Get Access to DropBox via OAuth:

    #https://www.dropbox.com/developers/apps  Get info about DropBox App

    #This DropBox App is called AutomatedRedCapEntry
    # Your app key and secret
    d_app_key = dropbox_app_key
    d_app_secret = dropbox_app_secret
    refresh_token = stored_refresh_token

    # #Your refresh token (previously obtained during the authorization process)
    # print(auth_url)
    # access_code = input("Please go to the website above and authenticate.  Copy the Code and paste here:")
    # # Define the data to be sent in the POST request
    # data = {
    #     'grant_type': 'authorization_code',
    #     'code': access_code,
    #     'client_id': d_app_key,
    #     'client_secret': d_app_secret
    # }
    # # Make the POST request to exchange the refresh token for a new access token
    # response = requests.post('https://api.dropboxapi.com/oauth2/token', data)

    # # Check if the request was successful and print the response
    # if response.status_code == 200:
    #     response_data = response.json()
    #     refresh_token = response_data['refresh_token']
    #     access_token = response_data['access_token']
    #     print(f"New access token: {access_token} and Refresh Token: {refresh_token}")
    # else:
    #     print(f"Error: {response.status_code} - {response.text}")

    try:
        # print("Hi")
        dbx = dropbox.Dropbox(oauth2_refresh_token=refresh_token, app_key=d_app_key, app_secret=d_app_secret)
        # Get information about the current user's account
        account_info = dbx.users_get_current_account()

        # # Access various properties of the user's account
        account_id = account_info.account_id
        display_name = account_info.name.display_name
        email = account_info.email
        is_verified = account_info.email_verified

        # Print the user's account information
        print(f"Account ID: {account_id}")
        print(f"Display Name: {display_name}")
        print(f"Email: {email}")

        dbx._session.verify = True

        # Specify the Dropbox folder path you want to check or create
        main_dropbox_folder_path = '/SaveRx/'
        second_dropbox_folder_path = '/SaveRx/NewDataFolder/'

        try:
        # List the contents of the folder
            result = dbx.files_list_folder(second_dropbox_folder_path)

            # Iterate through the entries and extract folder names
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FolderMetadata):
                    folder_name = entry.name
                    print(f"Folder: {folder_name}")
        except dropbox.exceptions.ApiError as e:
            print(f"Error listing folders: {e}")

        for folder in dbx.files_list_folder(second_dropbox_folder_path).entries:
            if folder.name == 'SharingFolderTest':
                # redcap_group = folder.name.lower()
                # print(redcap_group)
                dropbox_folder_path = f'{second_dropbox_folder_path}{folder.name}/'
                # print(f"The path for this current folder is {dropbox_folder_path}")
                print(f'The following new files were found in the Dropbox folder {dropbox_folder_path}:')
                # Get the list of files in the folder
                current_files = list_files_in_folder(dropbox_folder_path, dbx)
                #print(current_files)
                for nfile in current_files:
                    print(nfile)
                    #File Path of the current nfile
                    dropbox_file_path = f"{dropbox_folder_path}{nfile}"
                    print(f"Dropbox File Path: {dropbox_file_path}")

                    #Move the  original file to the file we want it to be in.
                    source_path = dropbox_file_path
                    destination_path = f'/AHRQ_R18_Project_SAVERx/bremo/{nfile}'
                    print(destination_path)
                    dbx.files_move_v2(source_path, destination_path)


    except AuthError as e:
        print("Error refreshing access token:", e)

if __name__ == "__main__":
    main()
