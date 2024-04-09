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
import BarrCleanExcelSaveRxFiles
import LSCleanExcelSaveRxFiles
from dropbox.exceptions import AuthError
import redcap
import os
import csv
from io import StringIO
import datetime
from io import BytesIO
import openpyxl
import pandas as pd
import pprint
import io
import tabula
import tempfile
import PyPDF2
import numpy as np
import math

#Set Pretty Print Rules to make the dictionaries look nice and easier to read when printed
pp = pprint.PrettyPrinter(indent=2, sort_dicts=False, width=100)

def read_csv_to_dicts(filepath, encoding='utf-8', newline='', delimiter=','):
    """
    Accepts a file path for a .csv file to be read, creates a file object,
    and uses csv.DictReader() to return a list of dictionaries
    that represent the row values from the file.

    Parameters:
        filepath (str): path to csv file
        encoding (str): name of encoding used to decode the file
        newline (str): specifies replacement value for newline '\n'
                    or '\r\n' (Windows) character sequences
        delimiter (str): delimiter that separates the row values

    Returns:
        list: nested dictionaries representing the file contents
     """

    with open(filepath, 'r', newline=newline, encoding='utf-8-sig') as file_obj:
        data = []
        reader = csv.DictReader(file_obj, delimiter=delimiter)
        for line in reader:
            data.append(line)
        return data

#Function to determine if a file is a .csv or a .xlsx
def get_file_type(file_path):
    """
    Takes the string that is the name of the file path and splits it at the '.' to determine if the file is 
    a csv file or an xlsx file.
    Parameters:
        Folder_path(string): The path to the folder
    Returns:
        xlsx, csv, pdf, or unknown (string)
    """
    _, file_extension = os.path.splitext(file_path)
    if file_extension == '.csv':
        return 'csv'
    elif file_extension == '.xlsx':
        return 'xlsx'
    elif file_extension == ".pdf":
        return 'pdf'
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

    #Your refresh token (previously obtained during the authorization process)
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
        #Access Dropbox
        dbx = dropbox.Dropbox(oauth2_refresh_token=refresh_token, app_key=d_app_key, app_secret=d_app_secret)
        # Get information about the current user's account
        account_info = dbx.users_get_current_account()

        # # Access various properties of the user's account, aka prove we got in to the account we want to
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
        file_prefix = 'NDCtoRXCUI'
        ndc_data = []
        print(f"The path for the Processed folder is: {processed_path}")

        ###This section specifies a path where a saved file of RxCUI information previously pulled from RxNorm would be if it exists. There is a try/except that looks for it  and writes a file if it isn't there.
        # Specify the Dropbox path for the file
        # This file is a saved, and updated, list of previous RxCUI dose forms, strengths and ingredients to prevent repeating API calls.
        dropbox_file_path = '/AHRQ_R18_Project_SAVERx/Processed/SaveRxCUIDetails.csv'
        #Look to see if the file exists already
        try:
            # Get metadata for the file
            metadata = dbx.files_get_metadata(dropbox_file_path)

            # File exists, so download it
            _, f = dbx.files_download(dropbox_file_path)

            #Use DictReader to bring the information into a list of dictionaries called rxcui_data
            rxcui_data = []
            csv_reader = csv.DictReader(f.content.decode().splitlines(), delimiter=',')
            for row in csv_reader:
                rxcui_data.append(row)
            print("Saved RxCUIData is Ready")
            rxcui_data = [{key.lstrip('\ufeff'): value for key, value in row.items()} for row in rxcui_data]

            print(rxcui_data)
        #if the file doesn't exist, create it
        except:
            print(f"The file {dropbox_file_path} does not exist.")
            #write csv This should only have the first time this program is run.
            new_file_name = "SaveRxCUIDetails.csv"
            saved_rxcui_path = f'{processed_path}SaveRxCUIDetails.csv'
            new_file_headers = ['RxCUI', 'RxNorm Name', 'RxNorm TTY', 'RxNorm Ingredient', 'RxNorm Dose Form', 'RxNorm Strength Details',
                                    'C RxNorm Name', 'C RxNorm TTY', 'C RxNorm RxCUI', 'C RxNorm Ingredient', 'C RxNorm Dose Form', 'C RxNorm Strength Details']
            starter_data = [{'RxCUI':'830717', 'RxNorm Name':'docosahexaenoic acid 200 MG / Eicosapentaenoic Acid 300 MG / Vitamin E 1 UNT Oral Capsule', 'RxNorm TTY':'SCD', 'RxNorm Ingredient':'',
                                'RxNorm Dose Form':'', 'RxNorm Strength Details':'', 'C RxNorm Name':'', 'C RxNorm TTY':'', 'C RxNorm RxCUI':'', 'C RxNorm Ingredient':'', 'C RxNorm Dose Form':'',
                                'C RxNorm Strength Details':''}]
            csv_buffer = StringIO()
            csv_writer = csv.DictWriter(csv_buffer, fieldnames=new_file_headers)
            # Write the CSV data to the buffer
            csv_writer.writeheader()
            csv_writer.writerows(starter_data)
            dbx.files_upload(csv_buffer.getvalue().encode(), saved_rxcui_path, mode=dropbox.files.WriteMode("overwrite"))

        ###This segment of code was built in case we wanted a csv file that would be active as a  "cache" for three months before being rebuilt
        # for folder in dbx.files_list_folder('/AHRQ_R18_Project_SAVERx/').entries:
        #     if folder.name == "Processed":
        #         #print("Yes, Processed Folder Exists")
        #         # Get a list of files in the folder with the specified prefix
        #         files_in_folder = list_files_in_folder(processed_path, dbx)
        #         matching_files = [file for file in files_in_folder if file.startswith(file_prefix)]
        #         print(matching_files)

        #         # Check if any matching files are found
        #         if matching_files:
        #             # Extract and parse the dates from each file
        #             file_dates = []
        #             for file in matching_files:
        #                 date_part = file[len(file_prefix):len(file_prefix) + 8]  # Assuming the date is 8 characters long
        #                 file_date = datetime.datetime.strptime(date_part, '%Y%m%d')
        #                 file_dates.append((file, file_date))

        #             # Find the file with the most recent date
        #             latest_file, latest_date = max(file_dates, key=lambda x: x[1])

        #             # Calculate the difference in months
        #             months_difference = (datetime.datetime.now().year - latest_date.year) * 12 + datetime.datetime.now().month - latest_date.month

        #             # Check if the difference is greater than 3 months
        #             if months_difference > 3:
        #                 print("The current date is more than 3 months after the date in the latest file:", latest_file)
        #                 #Erase Old File and Create a New One
        #                 print(f'{processed_path}{latest_file}')
        #                 dbx.files_delete(f'{processed_path}{latest_file}')
        #                 print(f"File '{latest_file}' has been deleted.")

        #                 #Create New File
        #                 new_file_name = f"NDCtoRXCUI{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        #                 new_file_path = f'{processed_path}{new_file_name}'
        #                 new_file_headers = ['NDC','RxCUI']
        #                 starter_data = [{'NDC':'52268010001','RxCUI':'966922'}, {'NDC':'781555531','RxCUI':'242438'}]
        #                 csv_buffer = StringIO()
        #                 csv_writer = csv.DictWriter(csv_buffer, fieldnames=new_file_headers)
        #                 # Write the CSV data to the buffer
        #                 csv_writer.writeheader()
        #                 csv_writer.writerows(starter_data)
        #                 dbx.files_upload(csv_buffer.getvalue().encode(), new_file_path, mode=dropbox.files.WriteMode("overwrite"))

        #                 print(f"New file '{new_file_name}' has been created and uploaded to Dropbox.")
        #             else:
        #                 print("The current date is not more than 3 months after the date in the latest file:", latest_file)
        #                 saved_ndcs = f'{processed_path}{latest_file}'
        #                 print(saved_ndcs)
        #                 # Download the file from Dropbox directly into memory
        #                 metadata, f = dbx.files_download(saved_ndcs)
        #                 # Parse the CSV data into a list of dictionaries
        #                 #ndc_data = []
        #                 csv_reader = csv.DictReader(f.content.decode().splitlines(), delimiter=',')
        #                 for row in csv_reader:
        #                     ndc_data.append(row)
        #                 print("Saved NDC/RxCUIData is Ready")
        #                 ndc_data = [{key.lstrip('\ufeff'): value for key, value in row.items()} for row in ndc_data]
        #                 print(ndc_data)
        #                 #Continue and use this file in the process. Use 'a' function

        #         else:
        #             print("No matching files found.")
        #             #Create New File
        #             new_file_name = f"NDCtoRXCUI{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        #             new_file_path = f'{processed_path}{new_file_name}'
        #             new_file_headers = ['NDC','RxCUI']
        #             starter_data = [{'NDC':'52268010001','RxCUI':'966922'}, {'NDC':'781555531','RxCUI':'242438'}]
        #             csv_buffer = StringIO()
        #             csv_writer = csv.DictWriter(csv_buffer, fieldnames=new_file_headers)
        #             # Write the CSV data to the buffer
        #             csv_writer.writeheader()
        #             csv_writer.writerows(starter_data)
        #             dbx.files_upload(csv_buffer.getvalue().encode(), new_file_path, mode=dropbox.files.WriteMode("overwrite"))

        #########################################

        ####Start the process of looking for new files sent by pharmacies that we need to process!
        for folder in dbx.files_list_folder('/AHRQ_R18_Project_SAVERx/').entries: #Look in our main folder to see what folders exist, other than Process and ReadyforRedcap (these our ours, not pharmacies)
            if folder.name != "Processed" and folder.name != "ReadyForRedcap":
                redcap_group = folder.name.lower()
                print(redcap_group)
                dropbox_folder_path = f'{main_dropbox_folder_path}{folder.name}/'
                print(f"The path for this current folder is {dropbox_folder_path}")
                print(f'The following new files were found in the Dropbox folder {dropbox_folder_path}:')
                # Get the list of files in the folder
                current_files = list_files_in_folder(dropbox_folder_path, dbx) #If there are files in any of the found folders, list what the files are as current files
                #print(current_files)
                for nfile in current_files: #process each file in current files one at a time
                    print(nfile)
                    name = nfile.split('.')[0]
                    #read in data from nfile to Process with RxNorm
                    dropbox_file_path = f"{dropbox_folder_path}{nfile}"
                    print(f"Dropbox File Path: {dropbox_file_path}")
                    #If the file is a .csv file, proceed this way:
                    file_type = get_file_type(dropbox_file_path)
                    ###This still needs to fix the headers.
                    if file_type == 'csv':
                        try:
                            # Download the file from Dropbox directly into memory
                            metadata, f = dbx.files_download(dropbox_file_path)

                            # Parse the CSV data into a list of dictionaries
                            data = []
                            # Decode the file content
                            file_content = f.content.decode()
                            # Create a file-like object using io.StringIO
                            file_obj = io.StringIO(file_content)
                            # Use csv.DictReader on the file-like object
                            csv_reader = csv.DictReader(file_obj, delimiter=',')

                            # Fix column names if necessary
                            column_names = csv_reader.fieldnames
                            if column_names and column_names[0].startswith('\ufeff'):
                                column_names[0] = column_names[0][1:]

                            row_id = 1
                            for row in csv_reader:
                                row['rowID'] = row_id
                                row_id += 1
                                data.append(row)
                            # Lowercase the headers
                            # Loop through each dictionary in the list
                            for d in data:
                                # Create a new dictionary to hold the modified key-value pairs
                                new_dict = {}
                                # Loop through key-value pairs in the dictionary
                                for key, value in d.items():
                                    # Convert the key to lowercase and add it to the new dictionary
                                    new_dict[key.lower()] = value
                                # Replace the original dictionary with the modified one
                                data[data.index(d)] = new_dict

                            pp.pprint(data[0:10])
                            print("Data is Ready to Head to Process Through SaveRx")
                            # Send to CachingProcessSaveRx the data from the pharmacy file, the name of this redcap_group, the name of the file (without the extension, and the saved rxcui_data)
                            ready_nfile, errors, second_errors, new_rxcui_data = CachingProcessSaveRx.main_process(data, redcap_group, name, rxcui_data)
                            print("Processing RxNorm information Complete! Onto uploading to Dropbox and Sending to Redcap")

                        except Exception as e:
                            print(f"Error processing {nfile.name}: {e}")

                        fieldnames = ['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4','erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength', 'page_number']

                        # Create a StringIO buffer to store the CSV data
                        csv_buffer = StringIO()
                        csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

                        # Write the CSV data to the buffer
                        csv_writer.writeheader()
                        csv_writer.writerows(ready_nfile)

                        #Create the filename of the new csv file that will be uploaded to dropbox for later import into redcap.
                        new_name = f'{os.path.splitext(name)[0]}ForRedcap.csv'
                        #Set the Dropbox path where I want the completed file stored
                        upload_dropbox_path = f'{main_dropbox_folder_path}ReadyForRedcap/{new_name}'
                        #Upload the csv file to dropbos
                        dbx.files_upload(csv_buffer.getvalue().encode(), upload_dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
                        print("CSV for RedCap Done")

                        #Move the completed original file to the Processed Folder in Dropbox since we are done with it.
                        # source_path = dropbox_file_path
                        # destination_path = f'/AHRQ_R18_Project_SAVERx/Processed/{nfile}'
                        # print(destination_path)
                        # dbx.files_move_v2(source_path, destination_path)

                        #Append new_rxcui_data to the rxcui_data
                        rxcui_data.extend(new_rxcui_data)
                        fieldnames2 = ['RxCUI', 'RxNorm Name', 'RxNorm TTY', 'RxNorm Ingredient', 'RxNorm Dose Form', 'RxNorm Strength Details',
                                         'C RxNorm Name', 'C RxNorm TTY', 'C RxNorm RxCUI', 'C RxNorm Ingredient', 'C RxNorm Dose Form', 'C RxNorm Strength Details']
                        new_file_name = "SaveRxCUIDetails.csv"
                        saved_rxcui_path = f'{processed_path}SaveRxCUIDetails.csv'
                        csv_buffer = StringIO()
                        csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames2)
                        # Write the CSV data to the buffer
                        csv_writer.writeheader()
                        csv_writer.writerows(rxcui_data)
                        dbx.files_upload(csv_buffer.getvalue().encode(), saved_rxcui_path, mode=dropbox.files.WriteMode("overwrite"))


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

                            headers = []
                            for cell in sheet[1]:
                                if cell.value is not None:
                                    headers.append(cell.value.lower())
                                else:
                                    headers.append('')
                            row_id = 1
                            for row in sheet.iter_rows(min_row=2, values_only=True):
                                # Create a dictionary with headers as keys and row values as values
                                row_data = {header: value for header, value in zip(headers, row)}
                                row_data['rowid'] = row_id
                                row_id += 1
                                data.append(row_data)
                            print(len(data))

                        except dropbox.exceptions.ApiError as e:
                            print(f"Error downloading XLSX file: {e}")
                        try:
                            pp.pprint(data[:5])
                            #Clean up the xslx file so it works nicely for us
                            if redcap_group == "barrs_hometown":
                                cleaned_up_file = BarrCleanExcelSaveRxFiles.main_process(data)
                            if redcap_group == "ls" or redcap_group == "manor_drug":
                                cleaned_up_file = LSCleanExcelSaveRxFiles.main_process(data)
                            else:
                                cleaned_up_file = CleanExcelSaveRxFiles.main_process(data)
                            # print("Cleaned and ready to go")
                            #Send to CachingProcessSaveRx the cleanedup_data from the pharmacy file, the name of this redcap_group, the name of the file (without the extension, and the saved rxcui_data)
                            ready_nfile, errors, second_errors, new_rxcui_data = CachingProcessSaveRx.main_process(cleaned_up_file, redcap_group, name, rxcui_data)
                            print(type(ready_nfile))
                        except Exception as e:
                            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            print(f"Error at {timestamp}: {e}")
                            # print(f"Error processing {ready_nfile.name}: {e}")

                        fieldnames = ['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4','erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength', 'page_number']

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

                        #Append new_rxcui_data to the rxcui_data
                        rxcui_data.extend(new_rxcui_data)
                        fieldnames2 = ['RxCUI', 'RxNorm Name', 'RxNorm TTY', 'RxNorm Ingredient', 'RxNorm Dose Form', 'RxNorm Strength Details',
                                         'C RxNorm Name', 'C RxNorm TTY', 'C RxNorm RxCUI', 'C RxNorm Ingredient', 'C RxNorm Dose Form', 'C RxNorm Strength Details']
                        new_file_name = "SaveRxCUIDetails.csv"
                        saved_rxcui_path = f'{processed_path}SaveRxCUIDetails.csv'
                        csv_buffer = StringIO()
                        csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames2)
                        # Write the CSV data to the buffer
                        csv_writer.writeheader()
                        csv_writer.writerows(rxcui_data)
                        dbx.files_upload(csv_buffer.getvalue().encode(), saved_rxcui_path, mode=dropbox.files.WriteMode("overwrite"))

                    ### Original Read in of PDF without page numbers
                    # elif file_type == 'pdf':
                    #     data = []
                    #     try:
                    #         # Download the PDF file directly into memory
                    #         metadata, response = dbx.files_download(dropbox_file_path)
                    #         pdf_data = response.content

                    #         # Create a temporary directory to store the PDF and CSV files
                    #         with tempfile.TemporaryDirectory() as temp_dir:
                    #             # Define file paths for the PDF and CSV files
                    #             pdf_file_path = os.path.join(temp_dir, "file.pdf")
                    #             csv_file_path = os.path.join(temp_dir, "file.csv")

                    #             # Write the PDF content to a temporary PDF file
                    #             with open(pdf_file_path, "wb") as pdf_file:
                    #                 pdf_file.write(pdf_data)

                    #             # Convert the PDF to CSV using tabula
                    #             tabula.convert_into(pdf_file_path, csv_file_path, output_format="csv", pages="all")

                    #             # Upload the CSV file back to Dropbox
                    #             with open(csv_file_path, "rb") as csv_file:
                    #                 # Set the destination path in Dropbox (same as original PDF file)
                    #                 destination_csv_path = dropbox_file_path.replace(".pdf", ".csv")
                    #                 # Upload the CSV file to Dropbox
                    #                 dbx.files_upload(csv_file.read(), destination_csv_path)
                    ###New Read In of PDF to Capture page numbers
                    elif file_type == 'pdf':
                        try:
                            # Download the PDF file directly into memory
                            metadata, response = dbx.files_download(dropbox_file_path)
                            pdf_data = response.content

                            data = []  # Initialize an empty list to collect data frames

                            # Create a temporary directory to store the PDF and processed files
                            with tempfile.TemporaryDirectory() as temp_dir:
                                # Define file paths for the PDF
                                pdf_file_path = os.path.join(temp_dir, "file.pdf")

                                # Write the PDF content to a temporary PDF file
                                with open(pdf_file_path, "wb") as pdf_file:
                                    pdf_file.write(pdf_data)

                                # Open PDF to get total number of pages
                                with open(pdf_file_path, "rb") as f:
                                    pdf_reader = PyPDF2.PdfReader(f)
                                    num_pages = len(pdf_reader.pages)

                                data = []  # Initialize an empty list to store DataFrames from all pages
                                all_data_dicts = []
#####
                                # Process the first page
                                first_page_df = tabula.read_pdf(pdf_file_path, pages=1, multiple_tables=False)[0]
                                if first_page_df['RxNumber'].isna().all():
                                    first_page_df = first_page_df.drop(columns=['RxNumber'])
                                headers = first_page_df.columns.tolist()  # save headers
                                # Convert headers to lowercase
                                headers = [header.lower() for header in headers]
                                first_page_dict_list = first_page_df.to_dict(orient='records')
                                # Add the page number to each dictionary
                                for d in first_page_dict_list:
                                    d['page_number'] = 1
                                all_data_dicts.extend(first_page_dict_list)  # append to all data

                                # Iterate through all dictionaries in the list
                                for i in range(len(all_data_dicts)):
                                    new_dict = {}  # Create a new dictionary to store modified keys and values
                                    for key, value in all_data_dicts[i].items():
                                        new_key = key.lower()  # Lowercase the key
                                        new_dict[new_key] = value  # Add the lowercase key and the value to the new dictionary
                                    all_data_dicts[i] = new_dict  # Replace the original dictionary with the new one

                                pp.pprint(all_data_dicts[0:3])
                                # Process subsequent pages
                                try:
                                    for page in range(2, num_pages + 1):
                                        df = tabula.read_pdf(pdf_file_path, pages=page, multiple_tables=False, pandas_options={'header': None})[0]
                                        # print(df.columns)
                                        pp.pprint(df)
                                        # Check that the DataFrame has the same number of columns; if not, adjust it
                                        if len(df.columns) == len(headers):  # Assuming headers includes 'page_number' that should not be applied
                                            # Set headers without 'page_number'
                                            df.columns = headers
                                        elif len(df.columns) < len(headers) - 1:
                                            # Add NaN columns if the DataFrame has fewer columns than expected
                                            for _ in range((len(headers) - 1) - len(df.columns)):
                                                df[pd.NA] = np.nan
                                            df.columns = headers[:-1]
                                        # Convert the current page DataFrame to a list of dictionaries
                                        current_page_dict_list = df.to_dict(orient='records')
                                        # pp.pprint(current_page_dict_list)

                                        # Add page number to each dictionary in the list
                                        for d in current_page_dict_list:
                                            d['page_number'] = page

                                        # Extend the all_data_dicts list with current page's dictionaries
                                        all_data_dicts.extend(current_page_dict_list)
                                    for entry in all_data_dicts:
                                        escript_ndc = entry.get('escript ndc')
                                        dscript_ndc = entry.get('dispensed ndc')
                                        # Check if the value is a number and not NaN
                                        if isinstance(escript_ndc, float) and not math.isnan(escript_ndc):
                                            # Convert to integer
                                            print(type(escript_ndc))
                                            entry['escript ndc'] = int(escript_ndc)
                                            print(type(entry['escript ndc']))
                                        else:
                                            # Handle NaN values (or any non-numeric values) here, for example, by setting to a default value or skipping the entry
                                            entry['escript ndc'] = None  # or some other default value, or don't modify the entry at all
                                        if isinstance(dscript_ndc, float) and not math.isnan(dscript_ndc):
                                            # Convert to integer
                                            entry['dispensed ndc'] = int(dscript_ndc)
                                        elif isinstance(dscript_ndc, int) and not math.isnan(dscript_ndc):
                                            entry['dispensed ndc'] = dscript_ndc
                                        else:
                                            # Handle NaN values (or any non-numeric values) here, for example, by setting to a default value or skipping the entry
                                            entry['dispensed ndc'] = None  # or some other default value, or don't modify the entry at all
                                    pp.pprint(all_data_dicts[0:3])


                                except Exception as e:
                                    print(f"An error occurred while processing page {page}: {e}, trying again")
                                    try:
                                        # Process subsequent pages
                                        df = tabula.read_pdf(pdf_file_path, pages=page, multiple_tables=False, pandas_options={'header': None})[0]
                                        # print(df.columns)
                                        pp.pprint(df)
                                        # Check that the DataFrame has the same number of columns; if not, adjust it
                                        if len(df.columns) == len(headers):  # Assuming headers includes 'page_number' that should not be applied
                                            # Set headers without 'page_number'
                                            df.columns = headers
                                        elif len(df.columns) < len(headers) - 1:
                                            # Add NaN columns if the DataFrame has fewer columns than expected
                                            for _ in range((len(headers) - 1) - len(df.columns)):
                                                df[pd.NA] = np.nan
                                            df.columns = headers[:-1]
                                        else:
                                            # Raise an error or handle the case where the DataFrame has more columns than expected
                                            raise ValueError(f"DataFrame has more columns than expected. Page: {page}")

                                        # Handle empty rows or rows where the expected key data is missing
                                        df.dropna(subset=['Expected Key Column'], inplace=True)  # Replace 'Expected Key Column' with the key column you expect
                                        print(df)

                                        # Convert the current page DataFrame to a list of dictionaries
                                        current_page_dict_list = df.to_dict(orient='records')

                                        # Add page number to each dictionary in the list
                                        for d in current_page_dict_list:
                                            d['page_number'] = page

                                        # Extend the all_data_dicts list with current page's dictionaries
                                        all_data_dicts.extend(current_page_dict_list)
                                         # Convert the current page DataFrame to a list of dictionaries
                                        # current_page_dict_list = df.to_dict(orient='records')
                                        # # Add page number to each dictionary in the list
                                        # for d in current_page_dict_list:
                                        #     d['page_number'] = page
                                        # # Extend the all_data_dicts list with current page's dictionaries
                                        # all_data_dicts.extend(current_page_dict_list)
                                        pp.pprint(all_data_dicts)
                                        for entry in all_data_dicts:
                                            escript_ndc = entry.get('escript ndc')
                                            dscript_ndc = entry.get('dispensed ndc')
                                            # Check if the value is a number and not NaN
                                            if isinstance(escript_ndc, float) and not math.isnan(escript_ndc):
                                                # Convert to integer
                                                entry['escript ndc'] = int(escript_ndc)
                                            else:
                                                # Handle NaN values (or any non-numeric values) here, for example, by setting to a default value or skipping the entry
                                                entry['escript ndc'] = None  # or some other default value, or don't modify the entry at all
                                            if isinstance(dscript_ndc, float) and not math.isnan(dscript_ndc):
                                                # Convert to integer
                                                entry['dispensed ndc'] = int(dscript_ndc)
                                            elif isinstance(dscript_ndc, int) and not math.isnan(dscript_ndc):
                                                entry['dispensed ndc'] = dscript_ndc
                                            else:
                                                # Handle NaN values (or any non-numeric values) here, for example, by setting to a default value or skipping the entry
                                                entry['dispensed ndc'] = None  # or some other default value, or don't modify the entry at all
                                        # pp.pprint(all_data_dicts[100:200])
                                            # Raise an error or handle the case where the DataFrame has more columns than expected
                                    except:
                                        ValueError(f"DataFrame has more columns than expected. Page: {page}")
                                        for entry in all_data_dicts:
                                            escript_ndc = entry.get('escript ndc')
                                            dscript_ndc = entry.get('dispensed ndc')
                                            # Check if the value is a number and not NaN
                                            if isinstance(escript_ndc, float) and not math.isnan(escript_ndc):
                                                # Convert to integer
                                                entry['escript ndc'] = int(escript_ndc)
                                            else:
                                                # Handle NaN values (or any non-numeric values) here, for example, by setting to a default value or skipping the entry
                                                entry['escript ndc'] = None  # or some other default value, or don't modify the entry at all
                                            if isinstance(dscript_ndc, float) and not math.isnan(dscript_ndc):
                                                # Convert to integer
                                                entry['dispensed ndc'] = int(dscript_ndc)
                                            elif isinstance(dscript_ndc, int) and not math.isnan(dscript_ndc):
                                                entry['dispensed ndc'] = dscript_ndc
                                            else:
                                                # Handle NaN values (or any non-numeric values) here, for example, by setting to a default value or skipping the entry
                                                entry['dispensed ndc'] = None  # or some other default value, or don't modify the entry at all

                                fieldnames = ['escript prescribed item', 'escript ndc', 'prescribed item', 'prescribed ndc', 'dispensed item', 'dispensed ndc', 'prescribed qty', 'quantity_unit', "recommended days' supply", 'prescribed refills', 'dispensed qty', "dispensed days' supply", 'page_number']
                                pp.pprint(all_data_dicts[-10:])
                                #########
                                correct_data_dicts = []
                                # correct_data_dicts.append(first_page_dict_list)
                                for i, d in enumerate(all_data_dicts):
                                    if all(key in fieldnames for key in d.keys()):  # Check if all keys in d are in the fieldnames
                                        correct_data_dicts.append(d)
                                    else:
                                        print(f"Dictionary at index {i} is removed because it contains keys not in fieldnames.")
                                ##########

                                # Set the destination path in Dropbox (replace the .pdf extension with .csv)
                                destination_csv_path = dropbox_file_path.replace(".pdf", ".csv")

                                # Create an in-memory text buffer to hold the CSV data
                                csv_buffer = io.StringIO()

                                fieldnames = ['escript prescribed item', 'escript ndc', 'prescribed item', 'prescribed ndc', 'dispensed item', 'dispensed ndc', 'prescribed qty', 'quantity_unit', "recommended days' supply", 'prescribed refills', 'dispensed qty', "dispensed days' supply", 'page_number']

                                # Create a csv.DictWriter object and write the list of dictionaries to the buffer
                                csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
                                csv_writer.writeheader()  # Write the header
                                csv_writer.writerows(correct_data_dicts)  # Write all dictionaries to CSV

                                # Convert the buffer's content to a string
                                csv_str = csv_buffer.getvalue()

                                # Upload the CSV string back to Dropbox
                                dbx.files_upload(csv_str.encode('utf-8'), destination_csv_path)

                                # Close the buffer
                                csv_buffer.close()

                                #Now read in the data from the csv file into a list of dictionaries
                                # Download the file from Dropbox directly into memory
                                metadata, f = dbx.files_download(destination_csv_path)

                                # Parse the CSV data into a list of dictionaries
                                data = []
                                # Decode the file content
                                file_content = f.content.decode()
                                # Create a file-like object using io.StringIO
                                file_obj = io.StringIO(file_content)
                                # Use csv.DictReader on the file-like object
                                csv_reader = csv.DictReader(file_obj, delimiter=',')

                                # Fix column names if necessary
                                column_names = csv_reader.fieldnames
                                if column_names and column_names[0].startswith('\ufeff'):
                                    column_names[0] = column_names[0][1:]
                                #Counter for rowID
                                row_id = 1
                                for row in csv_reader:
                                    #Add a new key-value pair for rowID to each row dictionary
                                    row['rowid'] = row_id
                                    row_id += 1
                                    data.append(row)
                                print("Data is Ready to Head to Process Through SaveRx")
                                #Send to CachingProcessSaveRx the cleanedup_data from the pharmacy file, the name of this redcap_group, the name of the file (without the extension, and the saved rxcui_data)
                                ready_nfile, errors, second_errors, new_rxcui_data = CachingProcessSaveRx.main_process(data, redcap_group, name, rxcui_data)
                                print(len(ready_nfile))

                                fieldnames = ['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4','erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength', 'page_number']

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

                                #Move the completed csv file to the Processed Folder in Dropbox since we are done with it.
                                csv_name = f'{os.path.splitext(name)[0]}.csv'
                                # source_path = destination_csv_path
                                destination_path2 = f'/AHRQ_R18_Project_SAVERx/bremo_retail/{csv_name}'
                                result = dbx.files_delete_v2(destination_path2) #Delete the csv file (we have kept the original pdf)
                                # print(destination_path2)
                                # dbx.files_move_v2(source_path, destination_path2)

                                #Append new_rxcui_data to the rxcui_data
                                rxcui_data.extend(new_rxcui_data)
                                fieldnames2 = ['RxCUI', 'RxNorm Name', 'RxNorm TTY', 'RxNorm Ingredient', 'RxNorm Dose Form', 'RxNorm Strength Details',
                                                    'C RxNorm Name', 'C RxNorm TTY', 'C RxNorm RxCUI', 'C RxNorm Ingredient', 'C RxNorm Dose Form', 'C RxNorm Strength Details']
                                new_file_name = "SaveRxCUIDetails.csv"
                                saved_rxcui_path = f'{processed_path}SaveRxCUIDetails.csv'
                                csv_buffer = StringIO()
                                csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames2)
                                # Write the CSV data to the buffer
                                csv_writer.writeheader()
                                csv_writer.writerows(rxcui_data)
                                dbx.files_upload(csv_buffer.getvalue().encode(), saved_rxcui_path, mode=dropbox.files.WriteMode("overwrite"))

                        except Exception as e:
                            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            print(f"Error at {timestamp}: {e}")
                            # print(f"Error processing {ready_nfile.name}: {e}")


    except AuthError as e:
            print("Error refreshing access token:", e)
    print("All Folders Checked, Process Complete")
# Main loop that runs daily
#     while True:


#         # Sleep for 24 hours (86400 seconds) before the next iteration
#         time.sleep(86400)




if __name__ == "__main__":
    main()
