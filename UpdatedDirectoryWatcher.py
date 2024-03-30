import dropbox
import time
import requests
from config import redcap_api_token
from config import dropbox_app_key
from config import dropbox_app_secret
from config import auth_url
from config import stored_refresh_token
from redcap import Project
import CachingProcessSaveRx
import CleanExcelSaveRxFiles
from dropbox.exceptions import AuthError
import redcap
import os
import csv
from io import StringIO
import datetime
from io import BytesIO
import openpyxl
import pandas as pd

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

def upload_csv_rows_to_redcap(api_url, api_key, csv_file_path, field, file_name, event=None, repeat_instance=None):
    """
    Upload each row of a CSV file to a REDCap project using PyCap's Project.import_file().

    Parameters:
        api_url (str): The API URL of the REDCap project.
        api_key (str): The API key for authentication.
        csv_file_path (str): The path to the CSV file to be uploaded.
        field (str): The field name where the file will go.
        file_name (str): The file name visible in the REDCap UI.
        event (str, optional): For longitudinal projects, the unique event name.
        repeat_instance (int or str, optional): The repeat instance number (for projects with repeating instruments/events).

    Returns:
        bool: True if all uploads are successful, False otherwise.
    """
    try:
        # Initialize the REDCap project
        project = redcap.Project(api_url, api_key)

        # Check if the project object is not None
        if project is not None:
            print("REDCap project initialized successfully.")
        else:
            print("Failed to initialize REDCap project.")

        # Check if the file exists
        if not os.path.isfile(csv_file_path):
            print(f"CSV file not found at {csv_file_path}.")
            return False

        # Read the CSV file with pandas
        df = pd.read_csv(csv_file_path)

        # Upload each row as a separate record
        successful_uploads = 0
        total_records = len(df)

        for index, row in df.iterrows():
            record = str(row['record'])  # Assuming 'record' is a column in the CSV
            file_to_upload = row['record_id']  # Replace 'file_path_column' with the actual CSV column name that contains file paths
            with open(file_to_upload, 'rb') as file_object:
                response = project.import_file(record, field, file_name, file_object, event, repeat_instance)


                # Check if the upload was successful
                if response['error'] is None:
                    print(f"File '{file_name}' uploaded successfully to REDCap for record {record}.")
                    successful_uploads += 1
                else:
                    print(f"Error uploading to REDCap for record {record}: {response['error']}")

        print(f"Uploaded {successful_uploads} out of {total_records} records successfully.")
        return successful_uploads == total_records

    except Exception as e:
        print(f"Error uploading to REDCap: {e}")
        return False

def upload_csv_rows_as_records(api_url, api_key, csv_file_path, event=None, repeat_instance=None):
    """
    Upload each row of a CSV file as a separate record to a REDCap project using PyCap's Project.import_records().

    Parameters:
        api_url (str): The API URL of the REDCap project.
        api_key (str): The API key for authentication.
        csv_file_path (str): The path to the CSV file to be uploaded.
        event (str, optional): For longitudinal projects, the unique event name.
        repeat_instance (int or str, optional): The repeat instance number (for projects with repeating instruments/events).

    Returns:
        bool: True if all uploads are successful, False otherwise.
    """
    try:
        # Initialize the REDCap project
        project = redcap.Project(api_url, api_key)

        # Check if the project object is not None
        if project is not None:
            print("REDCap project initialized successfully.")
        else:
            print("Failed to initialize REDCap project.")

        # Read the CSV file with pandas
        df = pd.read_csv(csv_file_path)

        # Upload each row as a separate record
        successful_uploads = 0
        total_records = len(df)

        for index, row in df.iterrows():
            # Convert the row to a dictionary and upload it as a record
            record_data = row.to_dict()
            response = project.import_records([record_data], event=event, repeat_instance=repeat_instance)

            # Check if the upload was successful
            if response['error'] is None:
                print(f"Record {row['record']} uploaded successfully to REDCap.")
                successful_uploads += 1
            else:
                print(f"Error uploading record {row['record']}: {response['error']}")

        print(f"Uploaded {successful_uploads} out of {total_records} records successfully.")
        return successful_uploads == total_records

    except Exception as e:
        print(f"Error uploading to REDCap: {e}")
        return False



def upload_to_redcap2(api_url, api_key, csv_file_path):
    """
    Uploads a CSV file to a REDCap project using redcap.Project.

    Parameters:
        api_url (str): The API URL of the REDCap project.
        api_key (str): The API key for authentication.
        csv_file_path (str): The path to the CSV file to be uploaded.

    Returns:
        bool: True if the upload is successful, False otherwise.
    """
    try:
        # Initialize the REDCap project
        project = redcap.Project(api_url, api_key)

        # Check if the project object is not None
        if project is not None:
            print("REDCap project initialized successfully.")
        else:
            print("Failed to initialize REDCap project.")

        # Check if the CSV file exists
        if not os.path.isfile(csv_file_path):
            print(f"CSV file not found at {csv_file_path}.")
            return False

        # Use PyCap's Project.import_file() to upload the CSV file
        response = project.import_file(csv_file_path, format="csv")

        # Check if the upload was successful
        if response['error'] is None:
            print(f"{response['message']} records uploaded successfully to REDCap.")
            return True
        else:
            print(f"Error uploading to REDCap: {response['error']}")
            return False

        # # Upload the CSV file to REDCap
        # response = project.import_records(csv_file_path, return_format_type = 'csv', import_format = 'csv')

        # # Check if the upload was successful
        # if response['count'] > 0:
        #     print(f"{response['count']} records uploaded successfully to REDCap.")
        #     return True
        # else:
        #     print("No records were uploaded to REDCap.")
        #     return False

    except Exception as e:
        print(f"Error uploading to REDCap: {e}")
        return False

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
        main_dropbox_folder_path = '/AHRQ_R18_Project_SAVERx/'
        processed_path = f'{main_dropbox_folder_path}Processed/'
        print(f"The path for the Processed folder is: {processed_path}")

        for folder in dbx.files_list_folder('/AHRQ_R18_Project_SAVERx/').entries:
            if folder.name != "Processed" and folder.name != "ReadyForRedcap":
                redcap_group = folder.name.lower()
                print(redcap_group)
                dropbox_folder_path = f'{main_dropbox_folder_path}{folder.name}/'
                print(f"The path for this current folder is {dropbox_folder_path}")
                print(f'The following new files were found in the Dropbox folder {dropbox_folder_path}:')
                # Get the list of files in the folder
                current_files = list_files_in_folder(dropbox_folder_path, dbx)
                #print(current_files)
                for nfile in current_files:
                    print(nfile)
                    name = nfile.split('.')[0]
                    #read in data from nfile to Process with RxNorm
                    dropbox_file_path = f"{dropbox_folder_path}{nfile}"
                    print(f"Dropbox File Path: {dropbox_file_path}")
                    #If the file is a .csv file, proceed this way:
                    file_type = get_file_type(dropbox_file_path)
                    if file_type == 'csv':
                        try:
                            # Download the file from Dropbox directly into memory
                            metadata, f = dbx.files_download(dropbox_file_path)

                            # Parse the CSV data into a list of dictionaries
                            data = []
                            csv_reader = csv.DictReader(f.content.decode().splitlines(), delimiter=',')
                            for row in csv_reader:
                                data.append(row)
                            print("Data is Ready to Head to Process Through SaveRx")
                            ready_nfile = CachingProcessSaveRx.main_process(data, redcap_group, name)
                            #ready_nfile, errors, second_errors = CachingProcessSaveRx.main_process(data, redcap_group, name)
                            print(type(ready_nfile))
                            #Upload the file from Dropbox to REDCAP
                            # project_url = 'https://redcapproduction.umms.med.umich.edu/api/'
                            # upload_to_redcap(project_url, redcap_api_token, ready_nfile)
                            # (ready_nfile))
                        except Exception as e:
                            print(f"Error processing {nfile.name}: {e}")

                        fieldnames = ['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4','erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength']

                        # Create a StringIO buffer to store the CSV data
                        csv_buffer = StringIO()
                        csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

                        # Write the CSV data to the buffer
                        csv_writer.writeheader()
                        csv_writer.writerows(ready_nfile)

                        new_name = f'{os.path.splitext(name)[0]}ForRedcap.csv'
                        # myFile4 = open(new_name, 'w', encoding='utf-8', newline='')
                        #Set the Dropbox path where I want the completed file stored
                        upload_dropbox_path = f'{main_dropbox_folder_path}ReadyForRedcap/{new_name}'
                        #Upload the csv file to dropbos
                        dbx.files_upload(csv_buffer.getvalue().encode(), upload_dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
                        print("CSV for RedCap Done")

                        #Move the completed original file to the Processed Folder in Dropbox since we are done with it.
                        source_path = dropbox_file_path
                        destination_path = f'/AHRQ_R18_Project_SAVERx/Processed/{nfile}'
                        print(destination_path)
                        dbx.files_move_v2(source_path, destination_path)

                        #Upload the file to redcap
                        #api_url, api_key, file_path, event=None, repeat_instance=None
                        recap_api_url = 'https://redcapproduction.umms.med.umich.edu/api/'
                        redcap_api_key = redcap_api_token
                        #upload_csv_rows_as_records(recap_api_url, redcap_api_key, destination_path)

                    elif file_type == 'xlsx':
                        data = []
                        try:
                            # Download the XLSX file directly into memory
                            metadata, response = dbx.files_download(dropbox_file_path)
                            xlsx_data = BytesIO(response.content)

                            # Load the XLSX data into openpyxl
                            workbook = openpyxl.load_workbook(xlsx_data)

                            # Process the XLSX data and read it into a list of dictionaries
                            sheet = workbook.active

                            headers = [cell.value for cell in sheet[1]]

                            for row in sheet.iter_rows(min_row=2, values_only=True):
                                # Create a dictionary with headers as keys and row values as values
                                row_data = {header: value for header, value in zip(headers, row)}
                                data.append(row_data)

                        except dropbox.exceptions.ApiError as e:
                            print(f"Error downloading XLSX file: {e}")
                        try:
                            cleaned_up_file = CleanExcelSaveRxFiles.main_process(data)
                            print("Cleaned and ready to go")
                            # ready_nfile = CachingProcessSaveRx.main_process(cleaned_up_file, redcap_group, name)
                            ready_nfile, errors, second_errors = CachingProcessSaveRx.main_process(cleaned_up_file, redcap_group, name)
                            print(type(ready_nfile))
                        except Exception as e:
                            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            print(f"Error at {timestamp}: {e}")
                            print(f"Error processing {nfile.name}: {e}")

                        fieldnames = ['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4','erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength']

                        # Create a StringIO buffer to store the CSV data
                        csv_buffer = StringIO()
                        csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

                        # Write the CSV data to the buffer
                        csv_writer.writeheader()
                        csv_writer.writerows(ready_nfile)

                        new_name = f'{os.path.splitext(name)[0]}ForRedcap.csv'
                        #Set the Dropbox path where I want the completed file stored
                        upload_dropbox_path = f'{main_dropbox_folder_path}ReadyForRedcap/{new_name}'
                        #Upload the csv file to dropbos
                        dbx.files_upload(csv_buffer.getvalue().encode(), upload_dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
                        print("CSV for RedCap Done")


                        #Move the completed original file to the Processed Folder in Dropbox since we are done with it.
                        source_path = dropbox_file_path
                        destination_path = f'/AHRQ_R18_Project_SAVERx/Processed/{nfile}'
                        print(destination_path)
                        dbx.files_move_v2(source_path, destination_path)

                        #Upload the file to redcap
                        #api_url, api_key, file_path, event=None, repeat_instance=None
                        recap_api_url = 'https://redcapproduction.umms.med.umich.edu/api/'
                        redcap_api_key = redcap_api_token
                        # upload_csv_rows_as_records(recap_api_url, redcap_api_key, destination_path)

                        # error_name = f'{os.path.splitext(name)[0]}Errors.txt'
                        # textfile_dropbox_path = f'/AHRQ_R18_Project_SAVERx/Processed/{error_name}'
                        # # Save the items in the text_data list to a text file
                        # with open(textfile_dropbox_path, 'w') as text_file:
                        #     text_file.write("\n".join(errors))

                        # second_error_name = f'{os.path.splitext(name)[0]}SecondErrors.txt'
                        # textfile_dropbox_path = f'/AHRQ_R18_Project_SAVERx/Processed/{second_error_name}'
                        # # Save the items in the text_data list to a text file
                        # with open(textfile_dropbox_path, 'w') as text_file:
                        #     text_file.write("\n".join(second_errors))


            # Save the current list of files for the next comparison
            # You would typically store this in a database or a file for future comparisons
            # Use files_move to move the file to the destination folder
            # Specify the name of the text file where you want to save the list
            file_list_filename = "file_list.txt"

            # # Open the text file in write mode
            # with open(file_list_filename, 'a') as file_list_file:
            #     # Write each file name to the text file, one per line
            #     for file_name in current_files:
            #         file_list_file.write(file_name + '\n')

            #     print(f"File list saved to {file_list_filename}")
            #     previous_files = current_files

    except AuthError as e:
            print("Error refreshing access token:", e)
    print("All Folders Checked, Process Complete")
# Main loop that runs daily
#     while True:


#         # Sleep for 24 hours (86400 seconds) before the next iteration
#         time.sleep(86400)




if __name__ == "__main__":
    main()
