import dropbox
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
import pprint
import io
import tabula
import tempfile
import PyPDF2
import numpy as np
import math
import requests
import csv
import pprint
from csv import DictWriter
from timeit import default_timer as timer
from collections import defaultdict, OrderedDict
from time import sleep
import time
import re

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

connect_timeout = 10
read_timeout = 100

class LFUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = {}  # Dictionary to store API responses
        self.frequency = defaultdict(int)  # Dictionary to store access frequencies
        self.order = OrderedDict()  # Ordered dictionary to maintain LFU order

    def get(self, key):
        if key not in self.cache:
            return None
        # Update the access frequency
        self.frequency[key] += 1
        # Move the key to the end of the LFU order
        self.order.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        if self.capacity == 0:
            return

        # Check if the cache is full and needs eviction
        if len(self.cache) >= self.capacity:
            # Find the least frequently used item
            lfu_key = min(self.frequency, key=self.frequency.get)
            # Remove the least frequently used item from the cache and frequency dictionary
            del self.cache[lfu_key]
            del self.frequency[lfu_key]
            del self.order[lfu_key]

        # Add the new key-value pair to the cache and update frequency
        self.cache[key] = value
        self.frequency[key] += 1
        # Add the key to the end of the LFU order
        self.order[key] = None

capacity = 100  # Set the capacity according to your requirements
lfu_cache = LFUCache(capacity)

def read_csv_to_dicts(filepath, encoding='utf-8-sig', newline='', delimiter=','):
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

    with open(filepath, 'r', newline=newline, encoding=encoding) as file_obj:
        data = []
        reader = csv.DictReader(file_obj, delimiter=delimiter)
        # Get the column names
        column_names = reader.fieldnames
        # Rename the first column if necessary
        if column_names and column_names[0].startswith('\ufeff'):
            column_names[0] = column_names[0][1:]
        for line in reader:
            data.append(line)
        return data

def get_rxnorm_rxcui(ndc, count):
    """
    Takes an NDC number, looks it up at the endpoint url (an RxNorm API)
    and looks to see if the NDC number exists and what its corresponding rxcui is
    Also has a clause for if the API doesn't work as expected
    Accepts the count, an integer, which is only used if there is a Response Error. If so it reports
    out the count to let the user know which entry/row of data caused the error. 

    Inputs: Integer (11 digit number)
    Outputs: Integer (rxcui)
    """
    if (len(ndc)) < 11:
            #Add 0s to the left of the number to ensure it's 11 digits long and then replace the NDC with the padded one
            padded_number = str(ndc).zfill(11)
            #item['Product Code (NDC)'] == padded_number
            connect_timeout = 10
            read_timeout = 100
            base_url = "https://rxnav.nlm.nih.gov/REST"
            endpoint = f"{base_url}/ndcstatus.json?ndc={padded_number}"
            max_try = 5
            try_number = 1
            while try_number <= max_try:
                response = requests.get(endpoint, timeout=(connect_timeout, read_timeout))
                if response.status_code == 200:
                    data = response.json()
                    #pp.pprint(data)
                    if "ndcStatus" in data and data["ndcStatus"] == "noRxcui":
                        print(f"No medication found for NDC: {ndc}")
                    elif "ndcStatus" in data and data["ndcStatus"] != "noRxcui":
                        rxcui = int(data["ndcStatus"]["rxcui"])
                    else:
                        print("Invalid response from API")
                    return rxcui
                else:
                    print(f"API failed on {endpoint}, entry number {count}, Trying Again, attempt number {try_number} of 5")
                    sleep(4)
                    try_number +=1
    else:
        ndc = ndc
        base_url = "https://rxnav.nlm.nih.gov/REST"
        endpoint = f"{base_url}/ndcstatus.json?ndc={ndc}"
        connect_timeout = 10
        read_timeout = 100
        max_try = 5
        try_number = 1
        while try_number <= max_try:
            response = requests.get(endpoint, timeout=(connect_timeout, read_timeout))
            try:
                if response.status_code == 200:
                    data = response.json()
                    #pp.pprint(data)
                    if "ndcStatus" in data and data["ndcStatus"] == "noRxcui":
                        print(f"No medication found for NDC: {ndc}")
                    elif "ndcStatus" in data and data["ndcStatus"] != "noRxcui":
                        rxcui = int(data["ndcStatus"]["rxcui"])
                    else:
                        print("Invalid response from API")
                    return rxcui
            except Exception as e:
                print(f"API failed on {endpoint}, entry number {count}, Trying Again, attempt number {try_number} of 5")
                sleep(4)
                try_number +=1

def get_dose_forms(rxcui_data):
    """
    Takes in the rxcui_data of information from the json pulled in from the RxNorm API using the RxCUI. The function adds the doseFormName
    and information from the doseFormGroupConcept(s) to the list of dose form info. The try/except is incase the doseFormGroupConcept info
    doesn't exist for a dictionary. Otherwise it counts the number of times doseFormGroupName is mentioned and then pulls the value from
    that key each time.

    Inputs: Dictionary of Data from the Json the API pulled in
    Outputs: List of Dose Form Information
    """
    if 'doseFormConcept' in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']:
        dose_form_options = []
        dose_form_options.append(rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormConcept'][0]['doseFormName'])
        if 'doseFormGroupName' in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormConcept'][0]:
            try:
                m = sum('doseFormGroupName' in item for item in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormGroupConcept'])
                n = 1
                while n <= m:
                    dose = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormGroupConcept'][n-1]['doseFormGroupName']
                    dose_form_options.append(dose)
                    n += 1
            except:
                pass
        elif 'doseFormGroupName' in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormGroupConcept'][0]:
            try:
                m = sum('doseFormGroupName' in item for item in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormGroupConcept'])
                n = 1
                while n <= m:
                    dose = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormGroupConcept'][n-1]['doseFormGroupName']
                    dose_form_options.append(dose)
                    n += 1
            except:
                pass
    else:
        dose_form_options = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormGroupConcept'][0]['doseFormGroupName']
    return dose_form_options

def get_multiple_ingredients(rxcui_data):
    """
    Takes in the rxcui_data of information from the json pulled in from the RxNorm API using the RxCUI. The function adds the Ingredient
    information from the activeIngredientName(s) to the list of ingredient info. This function is used when we know there is more than one
    ingredient for the medication.

    Inputs: Dictionary of Data from the Json the API pulled in
    Outputs: List of Ingredients
    """
    if 'ingredientAndStrength' in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']:
        ingredient_count = sum('activeIngredientName' in item for item in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'])
        n = 1
        ingredients = []
        while n <= ingredient_count:
            ingredients.append(rxcui_data["rxcuiStatusHistory"]["definitionalFeatures"]["ingredientAndStrength"][n-1]["activeIngredientName"])
            n += 1
    elif 'ingredientName' in rxcui_data["rxcuiStatusHistory"]["derivedConcepts"]["ingredientConcept"][0]:
            ingredients = rxcui_data["rxcuiStatusHistory"]["derivedConcepts"]["ingredientConcept"][0]['ingredientName']

    else:
            ingredients = ""
    return ingredients

def get_multiple_strengths(rxcui_data):
    """
    Takes in the rxcui_data of information from the json pulled in from the RxNorm API using the RxCUI. The function adds the strength
    information from the numerator and denominator values and units to the list of ingredient info. This function is used when we know there is more than one
    ingredient for the medication, so therefore more than one set of strength information.

    Inputs: Dictionary of Data from the Json the API pulled in
    Outputs: List of Strength Details
    """
    if 'ingredientAndStrength' in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']:
        ingredient_count = sum('activeIngredientName' in item for item in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'])
        n = 1
        strength_numerators = []
        strength_denominators = []
        while n <= ingredient_count:
            x = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][n-1]['numeratorValue']
            y = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][n-1]['numeratorUnit']
            a = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][n-1]['denominatorValue']
            b = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][n-1]['denominatorUnit']
            strength_numerators.append(f"{x} {y}")
            strength_denominators.append(f"{a} {b}")
            n += 1

        m = 0
        Strength_Details = []
        while m <= len(strength_numerators) - 1:
            numerator = strength_numerators[m]
            denominator = strength_denominators[m]
            Strength_Details.append(f"{numerator}/{denominator}")
            m += 1
    else:
        Strength_Details = ['N/A']
    return Strength_Details

def get_ingredient_strength_doseform(rxcui):
    """
    Takes the rxcui and finds the ingredient, strength, and dose form when looking for an RXCUI that is an remapped.

    Input: Integer (RxCUI)
    Output: Dictionary with the keys Remapped RxNorm Name, Remapped RxNorm TTY, Remapped RxNorm Ingredient, Remapped
    RxNorm Dose Form, Remapped RxNorm Strength Details. The values come from the RxCui given as the input.
    """
    rx_norm_info = {}
    url_rxcui = f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/historystatus.json'
    response = requests.get(url_rxcui, timeout=(connect_timeout, read_timeout))
    rxcui_data = response.json()
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rx_norm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rx_norm_info['RxNorm TTY'] = tty
    if rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
        ingredient = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
        dose_form = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormConcept'][0]['doseFormName']
        status = rxcui_data["rxcuiStatusHistory"]['metaData']['status']
        source = rxcui_data["rxcuiStatusHistory"]['metaData']['source']
        strength_numerator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
        strength_numeratorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
        strength_denominator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
        strength_denominatorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
        rx_norm_info["RxNorm Ingredient"] = ingredient
        rx_norm_info["RxNorm Dose Form"] = dose_form
        rx_norm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
    else:
        ingredient_count = sum('activeIngredientName' in item for item in rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'])
        dose_form = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['doseFormConcept'][0]['doseFormName']
        # print(f"Ingredient Count: {ingredient_count}")
        n = 1
        ingredients = []
        strength_numerators = []
        strength_denominators = []
        while n <= ingredient_count:
            ingredients.append(rxcui_data["rxcuiStatusHistory"]["definitionalFeatures"]["ingredientAndStrength"][n-1]["activeIngredientName"])
            x = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][n-1]['numeratorValue']
            y = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][n-1]['numeratorUnit']
            a = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][n-1]['denominatorValue']
            b = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][n-1]['denominatorUnit']
            strength_numerators.append(f"{x} {y}")
            strength_denominators.append(f"{a} {b}")
            n += 1
        rx_norm_info["RxNorm Ingredient"] = ingredients
        rx_norm_info["RxNorm Dose Form"] = dose_form
        m = 0
        Strength_Details = []
        while m <= len(strength_numerators) - 1:
            numerator = strength_numerators[m]
            denominator = strength_denominators[m]
            Strength_Details.append(f"{numerator}/{denominator}")
            m += 1
        rx_norm_info["RxNorm Strength Details"] = Strength_Details

    return(rx_norm_info)

def scd_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    status = rxcui_data["rxcuiStatusHistory"]['metaData']['status']
    if status != "NotCurrent" and status != "Remapped":
        #If the tty is an SCD, and there is only 1 ingredient to this product -
        if rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
            ingredient = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
            dose_form = get_dose_forms(rxcui_data)
            # print(f"Dose Form: {dose_form}")
            source = rxcui_data["rxcuiStatusHistory"]['metaData']['source']
            strength_numerator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
            strength_numeratorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
            strength_denominator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
            strength_denominatorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
            rxnorm_info["RxNorm Ingredient"] = ingredient
            rxnorm_info["RxNorm Dose Form"] = dose_form
            rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
        #If the tty is an SCD, and has multiple ingredients
        else:
            ingredients = get_multiple_ingredients(rxcui_data)
            dose_form = get_dose_forms(rxcui_data)
            Strength_Details = get_multiple_strengths(rxcui_data)
            rxnorm_info["RxNorm Ingredient"] = ingredients
            rxnorm_info["RxNorm Dose Form"] = dose_form
            rxnorm_info["RxNorm Strength Details"] = Strength_Details
    elif status == "Remapped":
        remapped_rxcui = rxcui_data['rxcuiStatusHistory']['derivedConcepts']['remappedConcept'][0]['remappedRxCui']
        remapped_url = f'https://rxnav.nlm.nih.gov/REST/rxcui/{remapped_rxcui}/historystatus.json'
        response3 = requests.get(remapped_url, timeout=(connect_timeout, read_timeout))
        remapped_rxcui_data = response3.json()
        remapped_tty = remapped_rxcui_data['rxcuiStatusHistory']['attributes']['tty']
        if remapped_rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
            ingredient = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
            dose_form = get_dose_forms(remapped_rxcui_data)
            strength_numerator = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
            strength_numeratorUnit = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
            strength_denominator = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
            strength_denominatorUnit = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
            rxnorm_info["RxNorm Ingredient"] = ingredient
            rxnorm_info["RxNorm Dose Form"] = dose_form
            rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
        #If the remappped_tty is an SCD has more than 1 ingredient
        elif remapped_rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "YES":
            ingredients = get_multiple_ingredients(remapped_rxcui_data)
            dose_form = get_dose_forms(remapped_rxcui_data)
            Strength_Details = get_multiple_strengths(remapped_rxcui_data)
            rxnorm_info["RxNorm Ingredient"] = ingredients
            rxnorm_info["RxNorm Dose Form"] = dose_form
            rxnorm_info["RxNorm Strength Details"] = Strength_Details
    else:
        rxnorm_info["RxNorm Ingredient"] = ''
        rxnorm_info["RxNorm Dose Form"] = ''
        rxnorm_info["RxNorm Strength Details"] = ''
    rxnorm_info['C RxNorm Name'] = ""
    rxnorm_info['C RxNorm TTY'] = ""
    rxnorm_info['C RxNorm RxCUI'] = ""
    rxnorm_info['C RxNorm Ingredient'] = ""
    rxnorm_info['C RxNorm Dose Form'] = ""
    rxnorm_info['C RxNorm Strength Details'] = ""
    return rxnorm_info

def sbd_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty

    status = rxcui_data["rxcuiStatusHistory"]['metaData']['status']
    if status != 'NotCurrent':
    # source = rxcui_data["rxcuiStatusHistory"]['metaData']['source']
    #If the Rxcui of the SBD isn't remapped
        if status != "Remapped":
        #If the SBD has only 1 ingredient
            if rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
                #Get all the original rxcui info
                ingredient = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
                dose_form = get_dose_forms(rxcui_data)
                strength_numerator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
                strength_numeratorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
                strength_denominator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
                strength_denominatorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
                rxnorm_info["RxNorm Ingredient"] = ingredient
                rxnorm_info["RxNorm Dose Form"] = dose_form
                rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
            #If the SBD has more than 1 ingredient
            elif rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "YES":
                #Get all the original rxcui info
                ingredients = get_multiple_ingredients(rxcui_data)
                dose_form = get_dose_forms(rxcui_data)
                Strength_Details = get_multiple_strengths(rxcui_data)
                rxnorm_info["RxNorm Ingredient"] = ingredients
                rxnorm_info["RxNorm Dose Form"] = dose_form
                rxnorm_info["RxNorm Strength Details"] = Strength_Details
            scd_rxcui = rxcui_data['rxcuiStatusHistory']['derivedConcepts']['scdConcept']['scdConceptRxcui']
            #Now find the RXNorm info from the SCD RxCui
            url_scd_cui = f'https://rxnav.nlm.nih.gov/REST/rxcui/{scd_rxcui}/historystatus.json'
            response2 = requests.get(url_scd_cui, timeout=(connect_timeout, read_timeout))
            scd_cui_data = response2.json()
            #If the SCD CUI isn't remapped:
            if scd_cui_data['rxcuiStatusHistory']['metaData']['status'] != "Remapped":
                #Get the tty of this data. We expect it to be an SCD
                scd_tty = scd_cui_data["rxcuiStatusHistory"]["attributes"]["tty"]
                #Get the name of the drug from this info
                scd_name = scd_cui_data["rxcuiStatusHistory"]["attributes"]["name"]
                #Get info from scd rx cui
                rxnorm_info["C RxNorm Name"] = scd_name
                rxnorm_info['C RxNorm TTY'] = scd_tty
                rxnorm_info["C RxNorm RxCUI"] = scd_rxcui
                #The scd cui only has one ingredient
                if scd_cui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
                    scd_ingredient = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
                    scd_dose_form = get_dose_forms(scd_cui_data)
                    scd_status = scd_cui_data["rxcuiStatusHistory"]['metaData']['status']
                    scd_source = scd_cui_data["rxcuiStatusHistory"]['metaData']['source']
                    scd_strength_numerator = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
                    scd_strength_numeratorUnit = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
                    scd_strength_denominator = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
                    scd_strength_denominatorUnit = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
                    rxnorm_info["C RxNorm Ingredient"] = scd_ingredient
                    rxnorm_info["C RxNorm Dose Form"] = scd_dose_form
                    rxnorm_info["C RxNorm Strength Details"] = f"{scd_strength_numerator} {scd_strength_numeratorUnit}/{scd_strength_denominator} {scd_strength_denominatorUnit}"
                #If the scd cui has more than one ingredient
                else:
                    #Get info from scd rx cui
                    scd_ingredients = get_multiple_ingredients(scd_cui_data)
                    scd_dose_form = get_dose_forms(scd_cui_data)
                    SCD_Strength_Details = get_multiple_strengths(scd_cui_data)
                    rxnorm_info["C RxNorm Ingredient"] = scd_ingredients
                    rxnorm_info["C RxNorm Dose Form"] = scd_dose_form
                    rxnorm_info["C RxNorm Strength Details"] = SCD_Strength_Details
            #If the scd_cui is remapped, get that information
            elif scd_cui_data['rxcuiStatusHistory']['status'] == "Remapped":
                rxnorm_info["C RxNorm RxCUI"] = scd_rxcui
                #Get the new rxcui of the remapped scd rxcui
                remapped_scd_cui = scd_cui_data['rxcuiStatusHistory']['derivedConcepts']['remappedConcept'][0]['remappedRxCui']
                details = get_ingredient_strength_doseform(remapped_scd_cui)
                rxnorm_info['C RxNorm Name'] = details.get('Remapped RxNorm Name')
                rxnorm_info['C RxNorm TTY'] = details.get('Remapped RxNorm TTY')
                rxnorm_info['C RxNorm Ingredient'] = details.get('Remapped RxNorm Ingredient')
                rxnorm_info['C RxNorm Dose Form'] = details.get('Remapped RxNorm Dose Form')
                rxnorm_info['C RxNorm Strength Details'] = details.get('Remapped RxNorm Strength Details')
                    #If the original rxcui remaps
        elif status == "Remapped":
            remapped_rxcui = rxcui_data['rxcuiStatusHistory']['derivedConcepts']['remappedConcept'][0]['remappedRxCui']
            remapped_url = f'https://rxnav.nlm.nih.gov/REST/rxcui/{remapped_rxcui}/historystatus.json'
            response3 = requests.get(remapped_url, timeout=(connect_timeout, read_timeout))
            remapped_rxcui_data = response3.json()
            remapped_tty = remapped_rxcui_data['rxcuiStatusHistory']['attributes']['tty']
            # print(remapped_tty)
            if remapped_tty == "SBD":
                #if the remapped_tty is an SBD with 1 ingredient
                if remapped_rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
                    ingredient = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
                    dose_form = get_dose_forms(remapped_rxcui_data)
                    strength_numerator = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
                    strength_numeratorUnit = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
                    strength_denominator = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
                    strength_denominatorUnit = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
                    rxnorm_info["RxNorm Ingredient"] = ingredient
                    rxnorm_info["RxNorm Dose Form"] = dose_form
                    rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
                #If the remappped_tty is an SBD has more than 1 ingredient
                elif remapped_rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "YES":
                    #Get all the original rxcui info
                    ingredients = get_multiple_ingredients(rxcui_data)
                    dose_form = get_dose_forms(rxcui_data)
                    Strength_Details = get_multiple_strengths(rxcui_data)
                    rxnorm_info["RxNorm Ingredient"] = ingredients
                    rxnorm_info["RxNorm Dose Form"] = dose_form
                    rxnorm_info["RxNorm Strength Details"] = Strength_Details
                    # get the SCD from the remapped SBD no matter the number of ingredients
                    scd_cui =  remapped_rxcui_data['rxcuiStatusHistory']['derivedConcepts']['scdConcept']['scdConceptRxcui']
                    scd_cui_url = f'https://rxnav.nlm.nih.gov/REST/rxcui/{scd_cui}/historystatus.json'
                    response4 = requests.get(scd_cui_url, timeout=(connect_timeout, read_timeout))
                    remapped_scd_cui_data = response4.json()
                    verify_tty = remapped_scd_cui_data['rxcuiStatusHistory']['attributes']['tty']
                    scd_status = remapped_scd_cui_data["rxcuiStatusHistory"]['metaData']['status']
                    rxnorm_info["C RxNorm RxCUI"] = scd_rxcui
                    # print(verify_tty)
                    #If the scd cui data has only 1 ingredient
                    if remapped_scd_cui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
                        scd_ingredient = remapped_scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
                        scd_dose_form = get_dose_forms(remapped_scd_cui_data)
                        scd_source = remapped_scd_cui_data["rxcuiStatusHistory"]['metaData']['source']
                        scd_strength_numerator = remapped_scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
                        scd_strength_numeratorUnit = remapped_scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
                        scd_strength_denominator = remapped_scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
                        scd_strength_denominatorUnit = remapped_scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
                        rxnorm_info["C RxNorm Ingredient"] = scd_ingredient
                        rxnorm_info["C RxNorm Dose Form"] = scd_dose_form
                        rxnorm_info["C RxNorm Strength Details"] = f"{scd_strength_numerator} {scd_strength_numeratorUnit}/{scd_strength_denominator} {scd_strength_denominatorUnit}"
                    #If the scd cui has more than one ingredient
                    else:
                        #Get info from scd rx cui
                        scd_ingredients = get_multiple_ingredients(remapped_scd_cui_data)
                        scd_dose_form = get_dose_forms(remapped_scd_cui_data)
                        SCD_Strength_Details = get_multiple_strengths(remapped_scd_cui_data)
                        rxnorm_info["C RxNorm Ingredient"] = scd_ingredients
                        rxnorm_info["C RxNorm Dose Form"] = scd_dose_form
                        rxnorm_info["C RxNorm Strength Details"] = SCD_Strength_Details
            elif remapped_tty == "SCD":
                scd_status = remapped_rxcui_data["rxcuiStatusHistory"]['metaData']['status']
                if scd_status != "Obsolete":
                    #If the remapped_tty is an SCD with 1 ingredient
                    if remapped_rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
                        ingredient = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
                        dose_form = get_dose_forms(remapped_rxcui_data)
                        strength_numerator = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
                        strength_numeratorUnit = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
                        strength_denominator = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
                        strength_denominatorUnit = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
                        rxnorm_info["RxNorm Ingredient"] = ingredient
                        rxnorm_info["RxNorm Dose Form"] = dose_form
                        rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
                    #If the remappped_tty is an SCD has more than 1 ingredient
                    elif remapped_rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "YES":
                        #Get all the original rxcui info
                        ingredients = get_multiple_ingredients(rxcui_data)
                        dose_form = get_dose_forms(rxcui_data)
                        Strength_Details = get_multiple_strengths(rxcui_data)
                        rxnorm_info["RxNorm Ingredient"] = ingredients
                        rxnorm_info["RxNorm Dose Form"] = dose_form
                        rxnorm_info["RxNorm Strength Details"] = Strength_Details
                elif scd_status == "Obsolete":
                    #If the remapped_tty is an SCD with 1 ingredient
                    if remapped_rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
                        ingredient = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
                        dose_form = get_dose_forms(remapped_rxcui_data)
                        strength_numerator = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
                        strength_numeratorUnit = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
                        strength_denominator = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
                        strength_denominatorUnit = remapped_rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
                        rxnorm_info["RxNorm Ingredient"] = ingredient
                        rxnorm_info["RxNorm Dose Form"] = dose_form
                        rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
                    #If the remappped_tty is an SCD has more than 1 ingredient
                    elif remapped_rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "YES":
                        #Get all the original rxcui info
                        ingredients = get_multiple_ingredients(remapped_rxcui_data)
                        dose_form = get_dose_forms(remapped_rxcui_data)
                        Strength_Details = get_multiple_strengths(remapped_rxcui_data)
                        rxnorm_info["RxNorm Ingredient"] = ingredients
                        rxnorm_info["RxNorm Dose Form"] = dose_form
                        rxnorm_info["RxNorm Strength Details"] = Strength_Details
            rxnorm_info["C RxNorm Name"] = ""
            rxnorm_info["C RxNorm TTY"] = ""
            rxnorm_info["C RxNorm RxCUI"] = ""
            rxnorm_info["C RxNorm Ingredient"] = ""
            rxnorm_info["C RxNorm Dose Form"] = ""
            rxnorm_info["C RxNorm Strength Details"] = ""

        # #If the rxcui_data['rxcuiStatusHistory']['status'] = something strange (i.e. empty)
        # else:
        #     rxnorm_info['RxNorm Name'] = " "
        #     rxnorm_info['RxNorm TTY'] = " "
        #     rxnorm_info['RxNorm Ingredient'] = " "
        #     rxnorm_info['RxNorm Dose Form'] = " "
        #     rxnorm_info['RxNorm Strength Details'] = " "
    elif status == "NotCurrent":
        rxnorm_info['RxNorm Ingredient'] = " "
        rxnorm_info['RxNorm Dose Form'] = " "
        rxnorm_info['RxNorm Strength Details'] = " "
        rxnorm_info["C RxNorm Name"] = ""
        rxnorm_info["C RxNorm TTY"] = ""
        rxnorm_info["C RxNorm RxCUI"] = ""
        rxnorm_info["C RxNorm Ingredient"] = ""
        rxnorm_info["C RxNorm Dose Form"] = ""
        rxnorm_info["C RxNorm Strength Details"] = ""

    return rxnorm_info

def gpck_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    #If the TTY is GPCK and has 1 ingredient
    if rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
        ingredient = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
        dose_form = get_dose_forms(rxcui_data)
        #print(f"Dose Form: {dose_form}")
        # status = rxcui_data["rxcuiStatusHistory"]['metaData']['status']
        source = rxcui_data["rxcuiStatusHistory"]['metaData']['source']
        strength_numerator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
        strength_numeratorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
        strength_denominator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
        strength_denominatorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
        rxnorm_info["RxNorm Ingredient"] = ingredient
        rxnorm_info["RxNorm Dose Form"] = dose_form
        rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
    else:
        #If the TTY is GPCK and has multiple ingredients
        ingredients = get_multiple_ingredients(rxcui_data)
        dose_form = get_dose_forms(rxcui_data)
        Strength_Details = get_multiple_strengths(rxcui_data)
        rxnorm_info["RxNorm Ingredient"] = ingredients
        rxnorm_info["RxNorm Dose Form"] = dose_form
        rxnorm_info["RxNorm Strength Details"] = Strength_Details
    #No matter the number of ingredients, get the pack_cui which is an SCD and get the scd info for this drug product
    pack_cui = rxcui_data['rxcuiStatusHistory']['pack']['packConcept'][0]["packRxcui"]
    #Now find the RXNorm info from the SCD RxCui
    url_scd_cui = f'https://rxnav.nlm.nih.gov/REST/rxcui/{pack_cui}/historystatus.json'
    response2 = requests.get(url_scd_cui, timeout=(connect_timeout, read_timeout))
    pack_cui_data = response2.json()
    #If the SCD CUI isn't remapped:
    if pack_cui_data['rxcuiStatusHistory']['metaData']['status'] != "Remapped":
        #Get the tty of this data. We expect it to be an SCD
        scd_tty = pack_cui_data["rxcuiStatusHistory"]["attributes"]["tty"]
        #Get the name of the drug from this info
        scd_name = pack_cui_data["rxcuiStatusHistory"]["attributes"]["name"]
        #Get info from scd rx cui
        rxnorm_info["C RxNorm Name"] = scd_name
        rxnorm_info["C RxNorm TTY"] = scd_tty
        rxnorm_info["C RxNorm RxCUI"] = pack_cui
        #The scd cui only has one ingredient
        if pack_cui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
            scd_ingredient = pack_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
            scd_dose_form = get_dose_forms(pack_cui_data)
            scd_status = pack_cui_data["rxcuiStatusHistory"]['metaData']['status']
            scd_source = pack_cui_data["rxcuiStatusHistory"]['metaData']['source']
            scd_strength_numerator = pack_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
            scd_strength_numeratorUnit = pack_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
            scd_strength_denominator = pack_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
            scd_strength_denominatorUnit = pack_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
            rxnorm_info["C RxNorm Ingredient"] = scd_ingredient
            rxnorm_info["C RxNorm Dose Form"] = scd_dose_form
            rxnorm_info["C RxNorm Strength Details"] = f"{scd_strength_numerator} {scd_strength_numeratorUnit}/{scd_strength_denominator} {scd_strength_denominatorUnit}"
        #If the scd cui has more than one ingredient
        else:
            #Get info from scd rx cui
            scd_ingredients = get_multiple_ingredients(pack_cui_data)
            scd_dose_form = get_dose_forms(pack_cui_data)
            SCD_Strength_Details = get_multiple_strengths(pack_cui_data)
            rxnorm_info["C RxNorm Ingredient"] = scd_ingredients
            rxnorm_info["C RxNorm Dose Form"] = scd_dose_form
            rxnorm_info["C RxNorm Strength Details"] = SCD_Strength_Details
    return rxnorm_info

def bpck_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    #If the BPCK has 1 Ingredient find the info
    if rxcui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
        ingredient = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
        dose_form = get_dose_forms(rxcui_data)
        # print(f"Dose Form: {dose_form}")
        # status = rxcui_data["rxcuiStatusHistory"]['metaData']['status']
        source = rxcui_data["rxcuiStatusHistory"]['metaData']['source']
        strength_numerator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
        strength_numeratorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
        strength_denominator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
        strength_denominatorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
        rxnorm_info["RxNorm Ingredient"] = ingredient
        rxnorm_info["RxNorm Dose Form"] = dose_form
        rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
    #If the BPCK has more than one ingredient, find the info
    else:
        ingredients = get_multiple_ingredients(rxcui_data)
        dose_form = get_dose_forms(rxcui_data)
        Strength_Details = get_multiple_strengths(rxcui_data)
        rxnorm_info["RxNorm Ingredient"] = ingredients
        rxnorm_info["RxNorm Dose Form"] = dose_form
        rxnorm_info["RxNorm Strength Details"] = Strength_Details
    #Find the SCD Info for this medication
    pack_cui = rxcui_data['rxcuiStatusHistory']['pack']['packConcept'][0]["packRxcui"]
    #Now find the RXNorm info from the SCD RxCui
    url_scd_cui = f'https://rxnav.nlm.nih.gov/REST/rxcui/{pack_cui}/historystatus.json'
    response2 = requests.get(url_scd_cui, timeout=(connect_timeout, read_timeout))
    scd_cui_data = response2.json()
    #If the SCD CUI isn't remapped:
    if scd_cui_data['rxcuiStatusHistory']['metaData']['status'] != "Remapped":
        #Get the tty of this data. We expect it to be an SCD
        scd_tty = scd_cui_data["rxcuiStatusHistory"]["attributes"]["tty"]
        #Get the name of the drug from this info
        scd_name = scd_cui_data["rxcuiStatusHistory"]["attributes"]["name"]
        #Get info from scd rx cui
        rxnorm_info["C RxNorm Name"] = scd_name
        rxnorm_info["C RxNorm TTY"] = scd_tty
        rxnorm_info["C RxNorm RxCUI"] = pack_cui
        #The scd cui only has one ingredient
        if scd_cui_data["rxcuiStatusHistory"]["attributes"]["isMultipleIngredient"] == "NO":
            scd_ingredient = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]["activeIngredientName"]
            scd_dose_form = get_dose_forms(scd_cui_data)
            scd_status = scd_cui_data["rxcuiStatusHistory"]['metaData']['status']
            scd_source = scd_cui_data["rxcuiStatusHistory"]['metaData']['source']
            scd_strength_numerator = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
            scd_strength_numeratorUnit = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
            scd_strength_denominator = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
            scd_strength_denominatorUnit = scd_cui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
            rxnorm_info["C RxNorm Ingredient"] = scd_ingredient
            rxnorm_info["C RxNorm Dose Form"] = scd_dose_form
            rxnorm_info["C RxNorm Strength Details"] = f"{scd_strength_numerator} {scd_strength_numeratorUnit}/{scd_strength_denominator} {scd_strength_denominatorUnit}"
        #If the scd cui has more than one ingredient
        else:
            #Get info from scd rx cui
            scd_ingredients = get_multiple_ingredients(scd_cui_data)
            scd_dose_form = get_dose_forms(scd_cui_data)
            SCD_Strength_Details = get_multiple_strengths(scd_cui_data)
            rxnorm_info["C RxNorm Ingredient"] = scd_ingredients
            rxnorm_info["C RxNorm Dose Form"] = scd_dose_form
            rxnorm_info["C RxNorm Strength Details"] = SCD_Strength_Details
    return rxnorm_info

def in_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    ingredient = rxcui_data["rxcuiStatusHistory"]["attributes"]["name"]
    dose_form = " "
    strength = " "
    rxnorm_info["RxNorm Ingredient"] = ingredient
    rxnorm_info["RxNorm Dose Form"] = dose_form
    rxnorm_info["RxNorm Strength Details"] = strength
    rxnorm_info['C RxNorm Name'] = ""
    rxnorm_info['C RxNorm TTY'] = ""
    rxnorm_info["C RxNorm RxCUI"] = "scd_rxcui"
    rxnorm_info['C RxNorm Ingredient'] = ""
    rxnorm_info['C RxNorm Dose Form'] = ""
    rxnorm_info['C RxNorm Strength Details'] = ""
    return rxnorm_info

def min_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    ingredients = get_multiple_ingredients(rxcui_data)
    dose_form = get_dose_forms(rxcui_data)
    Strength_Details = get_multiple_strengths(rxcui_data)
    rxnorm_info["RxNorm Ingredient"] = ingredients
    rxnorm_info["RxNorm Dose Form"] = dose_form
    rxnorm_info["RxNorm Strength Details"] = Strength_Details
    rxnorm_info['C RxNorm Name'] = ""
    rxnorm_info['C RxNorm TTY'] = ""
    rxnorm_info["C RxNorm RxCUI"] = ""
    rxnorm_info['C RxNorm Ingredient'] = ""
    rxnorm_info['C RxNorm Dose Form'] = ""
    rxnorm_info['C RxNorm Strength Details'] = ""
    return rxnorm_info

def scdg_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    ingredients = get_multiple_ingredients(rxcui_data)
    dose_form = get_dose_forms(rxcui_data)
    Strength_Details = get_multiple_strengths(rxcui_data)
    rxnorm_info["RxNorm Ingredient"] = ingredients
    rxnorm_info["RxNorm Dose Form"] = dose_form
    rxnorm_info["RxNorm Strength Details"] = Strength_Details
    rxnorm_info['C RxNorm Name'] = ""
    rxnorm_info['C RxNorm TTY'] = ""
    rxnorm_info["C RxNorm RxCUI"] = ""
    rxnorm_info['C RxNorm Ingredient'] = ""
    rxnorm_info['C RxNorm Dose Form'] = ""
    rxnorm_info['C RxNorm Strength Details'] = ""
    return rxnorm_info

def scdc_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    ingredient = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']["ingredientAndStrength"][0]["baseName"]
    strength_numerator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorValue']
    strength_numeratorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['numeratorUnit']
    strength_denominator = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorValue']
    strength_denominatorUnit = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']['ingredientAndStrength'][0]['denominatorUnit']
    rxnorm_info["RxNorm Ingredient"] = ingredient
    rxnorm_info["RxNorm Dose Form"] = ""
    rxnorm_info["RxNorm Strength Details"] = f"{strength_numerator} {strength_numeratorUnit}/{strength_denominator} {strength_denominatorUnit}"
    rxnorm_info['C RxNorm Name'] = ""
    rxnorm_info['C RxNorm TTY'] = ""
    rxnorm_info["C RxNorm RxCUI"] = ""
    rxnorm_info['C RxNorm Ingredient'] = ""
    rxnorm_info['C RxNorm Dose Form'] = ""
    rxnorm_info['C RxNorm Strength Details'] = ""
    return rxnorm_info

def sbdc_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    ingredient = rxcui_data["rxcuiStatusHistory"]['definitionalFeatures']["ingredientAndStrength"][0]["baseName"]
    ingredients = get_multiple_ingredients(rxcui_data)
    dose_form = get_dose_forms(rxcui_data)
    Strength_Details = get_multiple_strengths(rxcui_data)
    rxnorm_info["RxNorm Ingredient"] = ingredients
    rxnorm_info["RxNorm Dose Form"] = dose_form
    rxnorm_info["RxNorm Strength Details"] = Strength_Details
    rxnorm_info['C RxNorm Name'] = ""
    rxnorm_info['C RxNorm TTY'] = ""
    rxnorm_info["C RxNorm RxCUI"] = ""
    rxnorm_info['C RxNorm Ingredient'] = ""
    rxnorm_info['C RxNorm Dose Form'] = ""
    rxnorm_info['C RxNorm Strength Details'] = ""
    return rxnorm_info

def scdf_info(rxcui_data):
    """
    Take the given rxcui_data, from the api returned dictionary with information about the
    ingredient, dose form and strength and parse through it.

    Input: Dictionary of the response from pinging the RxNorm API
    Output: Dictionary {rx_info} with the keys RxNorm Ingredient, RxNorm Dose Form, RxNorm Strength Details
    and the values coming from the api call with information about the given rxcui
    """
    rxnorm_info = {}
    name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
    rxnorm_info['RxNorm Name'] = name
    tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
    rxnorm_info['RxNorm TTY'] = tty
    ingredients = rxcui_data["rxcuiStatusHistory"]['derivedConcepts']["ingredientConcept"][0]['ingredientName']
    dose_form = get_dose_forms(rxcui_data)
    Strength_Details = ""
    rxnorm_info["RxNorm Ingredient"] = ingredients
    rxnorm_info["RxNorm Dose Form"] = dose_form
    rxnorm_info["RxNorm Strength Details"] = Strength_Details
    rxnorm_info['C RxNorm Name'] = ""
    rxnorm_info['C RxNorm TTY'] = ""
    rxnorm_info["C RxNorm RxCUI"] = ""
    rxnorm_info['C RxNorm Ingredient'] = ""
    rxnorm_info['C RxNorm Dose Form'] = ""
    rxnorm_info['C RxNorm Strength Details'] = ""
    return rxnorm_info

def get_name_tty(rxcui):
    """
    Takes the rxcui and finds the ingredient, strength, and dose form when looking for an RXCUI that is an remapped.

    Input: Integer (RxCUI)
    Output: Dictionary with the keys Remapped RxNorm Name, Remapped RxNorm TTY, Remapped RxNorm Ingredient, Remapped
    RxNorm Dose Form, Remapped RxNorm Strength Details. The values come from the RxCui given as the input.
    """
    rx_norm_info = {}
    max_try = 5
    try_number = 1
    while try_number <= max_try:
        url_rxcui = f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/historystatus.json'
        response = requests.get(url_rxcui, timeout=(connect_timeout, read_timeout))
        try:
            if response.status_code == 200:
                rxcui_data = response.json()
                name = rxcui_data['rxcuiStatusHistory']['attributes']['name']
                rx_norm_info['RxNorm Name'] = name
                tty = rxcui_data['rxcuiStatusHistory']['attributes']['tty']
                rx_norm_info['RxNorm TTY'] = tty
                if tty == "SCD":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = scd_info(rxcui_data)
                elif tty == "SBD":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = sbd_info(rxcui_data)
                elif tty == "GPCK":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = gpck_info(rxcui_data)
                elif tty == "BPCK":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = bpck_info(rxcui_data)
                elif tty == "IN" or tty == "PIN":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = in_info(rxcui_data)
                elif tty == "MIN":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = min_info(rxcui_data)
                elif tty == "SCDG" or tty == "SBDG":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = scdg_info(rxcui_data)
                elif tty == "SCDC":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = scdc_info(rxcui_data)
                elif tty == "SBDC":
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info = sbdc_info(rxcui_data)
                elif tty == "SCDF":
                    rx_norm_info = scdf_info(rxcui_data)
                elif tty ==  "OCD" or tty == '':
                    #Do This to get Ingredient, Dose Form and Strength Details
                    rx_norm_info["RxNorm Ingredient"] = ""
                    rx_norm_info["RxNorm Dose Form"] = ""
                    rx_norm_info["RxNorm Strength Details"] = ""
                    rx_norm_info['C RxNorm Name'] = ""
                    rx_norm_info['C RxNorm TTY'] = ""
                    rx_norm_info["C RxNorm RxCUI"] = ""
                    rx_norm_info['C RxNorm Ingredient'] = ""
                    rx_norm_info['C RxNorm Dose Form'] = ""
                    rx_norm_info['C RxNorm Strength Details'] = ""
        # print(rx_norm_info)
            return rx_norm_info
        except:
            print(f'API failed on {url_rxcui}, Trying Again, attempt number {try_number} of 5')
            sleep(4)
            try_numner +=1

def clean_string(input_string):
    try:
        cleaned_string = input_string.replace('[', '').replace(']', '').replace("'", '')
        return cleaned_string
    except AttributeError:
        if isinstance(input_string, list):
            lstring = ', '.join(input_string)
            cleaned_string = lstring.replace("'", '')
            return cleaned_string

def fetch_data_from_api(key, count):
    """
    Pass in the NDC as the key (and 11 digit integer) and the count of which row of data this came from
    Spit back out the RxCUI, a multi digit integer
    """
    # Check if the data is in the cache
    cached_data = lfu_cache.get(key)
    if cached_data is not None:
        return cached_data

    # If not in the cache, make the API request
    api_data = get_rxnorm_rxcui(key, count)

    # Store the API response in the cache
    lfu_cache.put(key, api_data)

    return api_data

def fetch_rxnorm_data_from_api(key):
    """
    Input: Pass in the RxCUI (an integer) as the key,
    Output: Dictionary with the keys Remapped RxNorm Name, Remapped RxNorm TTY, Remapped RxNorm Ingredient, Remapped
    RxNorm Dose Form, Remapped RxNorm Strength Details. The values come from the RxCui given as the input.
    """
    # Check if the data is in the cache
    rx_cached_data = lfu_cache.get(key)
    if rx_cached_data is not None:
        return rx_cached_data

    # If not in the cache, make the API request
    api_rx_data = get_name_tty(key)

    # Store the API response in the cache
    lfu_cache.put(key, api_rx_data)

    return api_rx_data

# Function to check if entries exist in the truth table
def entries_are_true(entry1, entry2, truth_table):
    for row in truth_table:
        for key, value in row.items():
            if (entry1 == key and entry2 == value) or (entry1 == value and entry2 == key):
                return True
    return False

def check_entries_in_special_case_truth_table(entry1, entry2, special_case_truth_table):
    for row in special_case_truth_table:
        for key, value in row.items():
            if entry1 == key and entry2 == value:
                return True
    return False

def compare_medication_strengths(f, t):
    # Define regular expression pattern to extract numbers, units, and '/'
    pattern = r'(\d+(\.\d+)?)\s*(\w+)\s*/\s*(\d+(\.\d+)?)\s*(\w+)'

    # Use regular expression to find matches in both strings
    f_matches = re.findall(pattern, f)
    t_matches = re.findall(pattern, t)

    if not f_matches or not t_matches:
        print("Invalid medication strength format.")
        return False

    # Check if the number of strength/unit pairs is the same
    if len(f_matches) != len(t_matches):
        print("Number of medication strengths are different.")
        return False

    # Iterate over each strength/unit pair and compare them
    for f_match, t_match in zip(f_matches, t_matches):
        # Extract numbers and units and convert them to floats
        f_num1 = float(f_match[0])
        f_unit1 = f_match[2]
        f_num2 = float(f_match[3])
        f_unit2 = f_match[5]

        t_num1 = float(t_match[0])
        t_unit1 = t_match[2]
        t_num2 = float(t_match[3])
        t_unit2 = t_match[5]

        # Check if units are the same
        if f_unit1 != t_unit1 or f_unit2 != t_unit2:
            print("Units are different.")
            return False

        # Calculate ratios
        ratio_f = f_num1 / f_num2
        ratio_t = t_num1 / t_num2

        # Check if ratios are equal, half, or double
        if ratio_f != ratio_t and ratio_f * 2 != ratio_t and ratio_f * 0.5 != ratio_t:
            print("Medication strengths are different.")
            return False

    # If all comparisons pass, return True
    print("Medications have equal strength.")
    return True
##########################################################################################################################################################################
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
            if folder.name == "bremo_retail":
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
                    if file_type == 'pdf':
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

                                fieldnames = ['escript prescribed item', 'escript ndc', 'prescribed item', 'prescribed ndc', 'dispensed item', 'dispensed ndc', 'rxnumber', 'prescribed qty', 'quantity_unit', "recommended days' supply", 'prescribed refills', 'dispensed qty', "dispensed days' supply", 'page_number']
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

                                fieldnames = ['escript prescribed item', 'escript ndc', 'prescribed item', 'prescribed ndc', 'dispensed item', 'dispensed ndc', 'rxnumber', 'prescribed qty', 'quantity_unit', "recommended days' supply", 'prescribed refills', 'dispensed qty', "dispensed days' supply", 'page_number']

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

                                 ###This section of code gets the RxCuis' from the e and dscript NDCs###
                                brenmo_data = data
                                # brenmo_data = read_csv_to_dicts(file)
                                count = 0
                                addition = []
                                error_messages = []
                                new_rxcui_data = [] #This is where new RxCUI Api call info will be kept to send back and append the SavedRxCUIDetails.csv
                                #Record the Start Time for this process
                                start_time1 = time.time()
                                for entry in brenmo_data:
                                    print(entry)
                                    try:
                                        escript_ndc = entry.get('escript ndc')
                                        if escript_ndc is None or escript_ndc == '' or escript_ndc == 'None' or escript_ndc == 'Nan' or escript_ndc == 'none' or escript_ndc == 'nan':
                                            continue

                                        e_rxcui = fetch_data_from_api(escript_ndc, count)

                                        dispensed_ndc = entry.get('dispensed ndc')
                                        if dispensed_ndc != '' or dispensed_ndc.lower() == 'none' or dispensed_ndc.lower() == 'nan':
                                            d_rxcui = fetch_data_from_api(dispensed_ndc, count)
                                        else:
                                            d_rxcui = ""

                                        entry['e_rxcui'] = e_rxcui
                                        entry['d_rxcui'] = d_rxcui
                                    except Exception as e:
                                        error_message = (f"An error occurred for NDC {entry['escript ndc']} : {str(e)}")
                                        print(error_message)
                                        error_messages.append(error_message)
                                        continue
                                    new_dict = {
                                        'uniqueID': f"{name}{entry.get('rowid')}",
                                        'rowID': entry.get('rowid'),
                                        'reportID': name,
                                        'escript prescribed item': entry.get('escript prescribed item'),
                                        'escript ndc': entry.get('escript ndc'),
                                        # 'Prescribed Item': entry.get('Prescribed Item'),
                                        # 'Prescribed NDC': entry.get('Prescribed NDC'),
                                        'dispensed item': entry.get('dispensed item'),
                                        'dispensed ndc': entry.get('dispensed ndc'),
                                        'gcn': entry.get('gcn'),
                                        'E_rxcui': e_rxcui,
                                        # 'P_rxcui': p_rxcui,
                                        'D_rxcui': d_rxcui,
                                        'page_number': entry.get('page_number')
                                    }
                                    addition.append(new_dict)
                                    count += 1
                                    print(f"Found RxCUI for entry number {count}")
                                    # print(new_dict)
                                #Record the stop time for this process
                                stop_time1 = time.time()
                                pp.pprint(addition[0:2])
                                print("Finished Getting RxCUIs")
                                elapsed_time1 = stop_time1 - start_time1
                                print(f"The Time it took to get {len(brenmo_data)} rxcuis is {elapsed_time1}")
                                print("Compiled Data with RxCuis DONE")
                            # ###########################################################################################################
                            ###This section adds the RxNorm Details about the Ingredients, Strengths, and Dose Forms###
                                print("Starting to Process through SaveRx")
                                count = 0
                                added_rxnorm = []
                                second_set_error_messages = []
                                #Record the Start Time for this process
                                start_time2 = time.time()
                                e_matching_info = []
                                d_matching_info = []
                                for item in addition:
                                    if item.get('E_rxcui') != '' and item.get('E_rxcui') != None:
                                        #get the rxcui from the eprescription and use that to get drug info from rxnorm
                                        eRxCUI = item.get('E_rxcui')
                                        try:
                                            # Check if eRxCUI is in 'RxCUI' values in rxcui_data
                                            matching_items = [data for data in rxcui_data if data.get('RxCUI') == eRxCUI]
                                            # If there are matching items, add them to the result
                                            if matching_items:
                                                #print(matching_items)
                                                e_info = matching_items[0]
                                                print(e_info)

                                            else:
                                                # If information is not found, call the fetch_rxnorm_data_from_api function
                                                e_info = fetch_rxnorm_data_from_api(eRxCUI)
                                                print(e_info)
                                                #Append the New info to new_rxcui_data for updating the CSV file.
                                                new_rxcui_data.append({
                                                    'RxCUI': eRxCUI,
                                                    'RxNorm Name': e_info["RxNorm Name"],
                                                    'RxNorm TTY': e_info["RxNorm TTY"],
                                                    'RxNorm Ingredient': e_info["RxNorm Ingredient"],
                                                    'RxNorm Dose Form': e_info["RxNorm Dose Form"],
                                                    'RxNorm Strength Details': e_info["RxNorm Strength Details"],
                                                    'C RxNorm Name': e_info['C RxNorm Name'],
                                                    'C RxNorm TTY': e_info['C RxNorm TTY'],
                                                    'C RxNorm RxCUI': e_info['C RxNorm RxCUI'],
                                                    'C RxNorm Ingredient': e_info['C RxNorm Ingredient'],
                                                    'C RxNorm Dose Form': e_info['C RxNorm Dose Form'],
                                                    'C RxNorm Strength Details': e_info['C RxNorm Strength Details']
                                                })

                                        except:
                                            second_error_message = f"An error occured for RxCUI {item['E_rxcui']} at {item['rowID']}"
                                            print(second_error_message)
                                            second_set_error_messages.append(second_error_message)
                                            continue

                                    else:
                                        e_info = {}
                                        eRxCUI = "NA"
                                        e_info["RxNorm Name"] = ""
                                        e_info["RxNorm TTY"] = ""
                                        e_info["RxNorm Ingredient"] = ""
                                        e_info["RxNorm Dose Form"] = ""
                                        e_info["RxNorm Strength Details"] = ""
                                        e_info['C RxNorm Name'] = ""
                                        e_info['C RxNorm TTY'] = ""
                                        e_info['C RxNorm RxCUI'] = ""
                                        e_info['C RxNorm Ingredient'] = ""
                                        e_info['C RxNorm Dose Form'] = ""
                                        e_info['C RxNorm Strength Details'] = ""

                                    if item.get('D_rxcui') != ''and item.get('D_rxcui') != None:
                                        #get the rxcui from the eprescription and use that to get drug info from rxnorm
                                        dRxCUI = item.get('D_rxcui')
                                        try:
                                            # Check if dRxCUI is in 'RxCUI' values in rxcui_data
                                            matching_items = [data for data in rxcui_data if data.get('RxCUI') == dRxCUI]
                                            # If there are matching items, add them to the result
                                            if matching_items:
                                                #print(matching_items)
                                                d_info = matching_items[0]
                                                print(d_info)
                                            elif not matching_items:
                                                matching_items_new_rxcui_data = [data for data in new_rxcui_data if data.get('RxCUI') == dRxCUI]
                                                if matching_items_new_rxcui_data:
                                                    d_info = matching_items_new_rxcui_data[0]
                                                else:
                                                    # If information is not found, call the fetch_rxnorm_data_from_api function
                                                    d_info = fetch_rxnorm_data_from_api(dRxCUI)
                                                    #Append the New info to new_rxcui_data for updating the CSV file. if it wasn't just added with e_info above...
                                                    # if d_info.get('RxCUI') not in [data.get('RxCUI') for data in new_rxcui_data]:
                                                    new_rxcui_data.append({
                                                        'RxCUI': dRxCUI,
                                                        'RxNorm Name': d_info["RxNorm Name"],
                                                        'RxNorm TTY': d_info["RxNorm TTY"],
                                                        'RxNorm Ingredient': d_info["RxNorm Ingredient"],
                                                        'RxNorm Dose Form': d_info["RxNorm Dose Form"],
                                                        'RxNorm Strength Details': d_info["RxNorm Strength Details"],
                                                        'C RxNorm Name': d_info['C RxNorm Name'],
                                                        'C RxNorm TTY': d_info['C RxNorm TTY'],
                                                        'C RxNorm RxCUI': d_info['C RxNorm RxCUI'],
                                                        'C RxNorm Ingredient': d_info['C RxNorm Ingredient'],
                                                        'C RxNorm Dose Form': d_info['C RxNorm Dose Form'],
                                                        'C RxNorm Strength Details': d_info['C RxNorm Strength Details']
                                                    })

                                        except:
                                            second_error_message = f"An error occured for RxCUI {item['E_rxcui']} at {item['rowID']}"
                                            print(second_error_message)
                                            second_set_error_messages.append(second_error_message)
                                            continue

                                    else:
                                        d_info = {}
                                        dRxCUI = "NA"
                                        d_info["RxNorm Name"] = ""
                                        d_info["RxNorm TTY"] = ""
                                        d_info["RxNorm Ingredient"] = ""
                                        d_info["RxNorm Dose Form"] = ""
                                        d_info["RxNorm Strength Details"] = ""
                                        d_info['C RxNorm Name'] = ""
                                        d_info['C RxNorm TTY'] = ""
                                        d_info['C RxNorm RxCUI'] = ""
                                        d_info['C RxNorm Ingredient'] = ""
                                        d_info['C RxNorm Dose Form'] = ""
                                        d_info['C RxNorm Strength Details'] = ""
                                    count += 1
                                    print(count)
                                    print(e_info["C RxNorm RxCUI"])
                                    added_rxnorm.append({
                                        "uniqueID": item.get('uniqueID'),
                                        "rowID": item.get('rowID'),
                                        "reportID": item.get('reportID'),
                                        "eNDC": item.get('escript ndc'),
                                        "eRxCUI": item.get('E_rxcui'),
                                        "original_erx_med_name": item.get('escript prescribed item'),
                                        "dNDC": item.get("dispensed ndc"),
                                        "dRxCUI": item.get('D_rxcui'),
                                        "pharm_Dispensed_med": item.get('dispensed item'),
                                        "GCN": item.get('gcn'),
                                        "eRxNorm Name": e_info["RxNorm Name"],
                                        "eRxNorm TTY": e_info["RxNorm TTY"],
                                        "eRxNorm Ingredient": e_info["RxNorm Ingredient"],
                                        "eRxNorm Dose Form": e_info["RxNorm Dose Form"],
                                        "eRxNorm Strength Details": e_info["RxNorm Strength Details"],
                                        "eSCD RxNorm Name": e_info["C RxNorm Name"],
                                        "eSCD RxNorm TTY": e_info["C RxNorm TTY"],
                                        "eSCD RxCUI": e_info["C RxNorm RxCUI"],
                                        "eSCD RxNorm Ingredient": e_info["C RxNorm Ingredient"],
                                        "eSCD RxNorm Dose Form": e_info["C RxNorm Dose Form"],
                                        "eSCD RxNorm Strength Details": e_info["C RxNorm Strength Details"],
                                        "dRxNorm Name": d_info["RxNorm Name"],
                                        "dRxNorm TTY": d_info["RxNorm TTY"],
                                        "dRxNorm Ingredient": d_info["RxNorm Ingredient"],
                                        "dRxNorm Dose Form": d_info["RxNorm Dose Form"],
                                        "dRxNorm Strength Details": d_info["RxNorm Strength Details"],
                                        "dSCD RxNorm Name": d_info["C RxNorm Name"],
                                        "dSCD RxNorm TTY": d_info["C RxNorm TTY"],
                                        "dSCD RXCUI": d_info["C RxNorm RxCUI"],
                                        "dSCD RxNorm Ingredient": d_info["C RxNorm Ingredient"],
                                        "dSCD RxNorm Dose Form": d_info["C RxNorm Dose Form"],
                                        "dSCD RxNorm Strength Details": d_info["C RxNorm Strength Details"],
                                        "page_number": item.get('page_number')
                                    }) #32 keys

                                #Record the stop time for this process
                                stop_time2 = time.time()
                                pp.pprint(added_rxnorm)
                                print("Finished Getting RxCUIs")
                                elapsed_time2 = stop_time2 - start_time2
                                print(f"The Time it took to get {len(addition)} rxnorm info is {elapsed_time2}")
                                # pp.pprint(added_rxnorm)
                                print("CSV with RxNorm Data DONE")

                                ####CLEAN UP#####
                                ##Do this once to clean up the data, then save the file and go from there##
                                # bremo =read_csv_to_dicts(f'{directory}compiled_data_withRxCUI_RxNormCUIS.csv')

                                for item in added_rxnorm:
                                    item['eRxNorm Ingredient'] = clean_string(item.get('eRxNorm Ingredient'))
                                    item['eRxNorm Dose Form'] = clean_string(item.get('eRxNorm Dose Form'))
                                    item['eRxNorm Strength Details'] = clean_string(item.get('eRxNorm Strength Details'))
                                    item['eSCD RxNorm Ingredient'] = clean_string(item.get('eSCD RxNorm Ingredient'))
                                    item['eSCD RxNorm Dose Form'] = clean_string(item.get('eSCD RxNorm Dose Form'))
                                    item['eSC DRxNorm Strength Detail'] = clean_string(item.get('eSCD RxNorm Strength Detail'))
                                    item['dRxNorm Ingredient'] = clean_string(item.get('dRxNorm Ingredient'))
                                    item['dRxNorm Dose Form'] = clean_string(item.get('dRxNorm Dose Form'))
                                    item['dRxNorm Strength Details'] = clean_string(item.get('dRxNorm Strength Details'))
                                    item['dSCD RxNorm Ingredient'] = clean_string(item.get('dSCD RxNorm Ingredient'))
                                    item['dSCD RxNorm Dose Form'] = clean_string(item.get('dSCD RxNorm Dose Form'))
                                    item['dSCD RxNorm Strength Details'] = clean_string(item.get('dSCD RxNorm Strength Details'))
                                    # pp.pprint(item)

                                ##################################################################################################################
                                ###REDCAP CLEAN UP###################
                                ###Step 1
                                ##Secondary Matching, including SCDs
                                # redcap_rxcui_compare = read_csv_to_dicts(f'{directory}completed_data.csv')
                                # print(len(redcap_rxcui_compare))
                                has_eNDCtoo = []
                                for item in added_rxnorm:
                                    if item.get('eNDC') != "":
                                        has_eNDCtoo.append(item)
                                print(f"The number of items that have both an escript description and NDC number are {len(has_eNDCtoo)}.")

                                ###Step 2 Filter out compounds
                                compounds  = []
                                for item in added_rxnorm:
                                    if item.get('dispensed item') is not None:
                                        if "cmpd" in item.get('dispensed item').lower() and item.get('dispensed ndc') is None:
                                            compounds.append(item)
                                print(len(compounds))

                                non_compounds = [item for item in has_eNDCtoo if item not in compounds]

                                ### Step 2.5 Add truth table of values for equivalent dose forms, and salt forms
                                ing_truth_table = [
                                    {"hydroxyzine pamoate": "hydroxyzine hydrochloride"},
                                    {"doxycycline hyclate": "doxycycline monohydrate"},
                                    {"desvenlafaxine": "desvenlafaxine succinate"},
                                    {"bacitracin": "bacitracin zinc"}
                                ]

                                chewable_ing_truth_table = ["ascorbic acid", "loratadine", "melatonin", "cetrizine hydrochloride", "calcium ion, cholecalciferol"]

                                df_truth_table = [
                                    {"Auto-Injector, Injectable Product": "Cartridge, Injectable Product"},
                                    {'Auto-Injector, Injectable Product': 'Prefilled Syringe, Injectable Product'},
                                    {"Delayed Release Oral Tablet, Oral Product, Pill": "Delayed Release Oral Capsule, Oral Product, Pill"},
                                    {"Extended Release Oral Tablet, Oral Product, Pill": "Extended Release Oral Capsule, Oral Product, Pill"},
                                    {"Injection, Injectable Product": "Prefilled Syringe, Injectable Product"},
                                    {"Injection, Injectable Product": "Cartridge, Injectable Product"},
                                    {"Injectable Solution, Injectable Product": "Pen Injector, Injectable Product"},
                                    {"Injectable Solution, Injectable Product": "Injection, Injectable Product"},
                                    {"Injectable Solution, Injectable Product": "Prefilled Syringe, Injectable Product"},
                                    {"Injectable Suspension, Injectable Product": "Injection, Injectable Product"},
                                    {"Injectable Suspension, Injectable Product": "Prefilled Suspension, Injectable Product"},
                                    {"Ophthalmic Solution, Ophthalmic Product": "Ophthalmic Gel, Ophthalmic Product"},
                                    {"Oral Capsule, Oral Product, Oral Pill": "Oral Tablet, Oral Product, Pill"},
                                    {"Oral Capsule, Oral Product, Oral Pill": "Pill"},
                                    {"Oral Solution, Oral Product, Oral Liquid Product": "Oral Suspension, Oral Product, Oral Liquid Product"},
                                    {"Oral Tablet, Oral Product, Pill": "Pill"},
                                    {"Oral Tablet, Oral Product, Pill": "Pack, Oral Product, Pill"},
                                    {"Oral Tablet, Oral Product, Pill": "Oral Capsule, Oral Product, Pill"},
                                    {"Otic Solution, Otic Product": "Otic Suspension, Otic Product"},
                                    {"Pack, Topical Product, Transdermal Product": "Transdermal System, Topical Product, Transdermal Product"},
                                    {"Prefilled Syringe, Injectable Product": "Cartridge, Injectable Product"},
                                    {"Sublingual Film, Oral Product, Sublingual Product, Oral Film Product": "Sublingual Tablet, Oral Product, Pill, Sublingual Product"},
                                    {"Topical Cream, Topical Product": "Topical Ointment, Topical Product"},
                                    {"Topical Foam, Topical Product": "Topical Gel, Topical Product"},
                                    {"Topical Lotion, Topical Product": "Topical Cream, Topical Product"},
                                    {"Topical Solution, Topical Product": "Topical Cream, Topical Product"}
                                                ]

                                special_case_truth_table = [
                                    {"Otic Solution, Otic Product": "Ophthalmic Solution, Ophthalmic Product"},
                                    {"Otic Solution, Otic Product": "Ophthalmic Suspension, Ophthalmic Product"},
                                    {"Otic Suspension, Otic Product": "Ophthalmic Solution, Ophthalmic Product"},
                                    {"Otic Suspension, Otic Product": "Ophthalmic Suspension, Ophthalmic Product"}
                                ]

                                schedule_2_table = ['alfentanil', 'alfentanil hydrochloride', 'alphaprodine', 'alphaprodine hydrochloride', 'amobarbital', 'amobarbital sodium',
                                                    'amphetamine', 'amphetamine aspirate', 'amphetamine aspirate monohydrate', 'amphetamine saccharate', 'amphetamine sulfate',
                                                    'dextroamphetamine', 'dextroamphetamine sulfate', 'anileridine', 'anileridine hydrochloride', 'cocaine', 'cocaine epinephrine',
                                                    'cocaine homatropine', 'codeine', 'codeine anhyd', 'codeine anhydrous', 'codeine hydrochloride', 'codeine phosphate', 'codeine phosphate anhydrous',
                                                    'codeine polistirex', 'codeine sulfate', 'dihydrocodeine', 'dihydrocodeine bitartate', 'dronabinol', 'ethylmorphine', 'ethylmorphine hydrochloride',
                                                    'fentanyl', 'fentanyl citrate', 'fentanyl hydrochloride', 'glutethimide', 'hydrocodone', 'hydrocodone bitartrate', 'hydrocodone hydrochloride',
                                                    'hydrocodone polistirex', 'hydrocodone resin complex', 'hydrocodone tannate', 'hydromorphone', 'hydromorphone hydrochloride', 'levorphanol tartrate',
                                                    'levorphanol', 'lisdexamfetamine dimesylate', 'lisdexamfetamine', 'meperidine', 'meperidine hydrochloride', 'methadone', 'methadone hydrochloride',
                                                    'methamphetamine', 'methamphetamine hydrochloride', 'methylphenidate', 'methylphenidate hydrochloride', 'morphine', 'morphine hydrochloride',
                                                    'morphine liposomal', 'morphine sulfate', 'morphine sulfate liposomal', 'morphone tartrate', 'nabilone', 'oliceridine', 'oliceridine fumarate',
                                                    'opium', 'opium tincture', 'oxycodone', 'oxycodone hydrochloride, oxycodone terephthalate', 'oxymorphone', 'oxymorphone hydrochloride', 'pentobarbital',
                                                    'pentobarbital sodium', 'phenazocine', 'phenazocine hydrobromide', 'phenmetrazine', 'phenmetrazine hydrochloride', 'phenmetrazine theoclate',
                                                    'remifentanil', 'remifentanil hydrochloride', 'secobarbital', 'secobarbital sodium', 'sufentanil', 'sufentanil citrate', 'tapentadol', 'tapentadol hydrochloride']

                            #Step 3 Get keys that we will to make matches with
                                filtered_for_redcap_withSCDS = []
                                for item in non_compounds:
                                    a = item.get('eNDC')
                                    b = item.get('eRxCUI')
                                    c = item.get('eRxNorm TTY')
                                    d = item.get('eRxNorm Ingredient')
                                    e = item.get('eRxNorm Dose Form')
                                    f = item.get('eRxNorm Strength Details')
                                    g = item.get('eSCD RxCUI')
                                    h = item.get('eSCD RxNorm Ingredient')
                                    i = item.get('eSCD RxNorm Dose Form')
                                    j = item.get('eSCD RxNorm Strength Detail')
                                    k = item.get('dNDC')
                                    l = item.get('dRxCUI')
                                    m = item.get('dRxNorm TTY')
                                    n = item.get('dRxNorm Ingredient')
                                    o = item.get('dRxNorm Dose Form')
                                    p = item.get('dRxNorm Strength Details')
                                    q = item.get('dSCD RxCUI')
                                    r = item.get('dSCD RxNorm Ingredient')
                                    s = item.get('dSCD RxNorm Dose Form')
                                    t = item.get('dSCD RxNorm Strength Details')

                                    match_details = ''
                                    incorrect_action1 = ''
                                    incorrect_action2 = ''
                                    incorrect_action3 = ''
                                    incorrect_action4 = ''
                                    if a == k: #If NDC's match, rxcuis match
                                        incorrect_action1 = 0
                                        incorrect_action2 = 0
                                        incorrect_action3 = 0
                                        incorrect_action4 = 0
                                        match_details = "2"
                                    elif b == l: # if eRxcui == dRxCUI, rxcuis match
                                        incorrect_action1 = 0
                                        incorrect_action2 = 0
                                        incorrect_action3 = 0
                                        incorrect_action4 = 0
                                        match_details = "2"
                                    elif "b" in c.lower(): #IF the TTY is branded, compare SCD versions
                                        if b == q and q != "": # if erxcui matches dSCD rxcui
                                            incorrect_action1 = 0
                                            incorrect_action2 = 0
                                            incorrect_action3 = 0
                                            incorrect_action4 = 0
                                            match_details = "3"
                                        elif  g == l and q != "": #if escd rxcui matches drxcui, rxcuis match
                                            incorrect_action1 = 0
                                            incorrect_action2 = 0
                                            incorrect_action3 = 0
                                            incorrect_action4 = 0
                                            match_details = "3"
                                        elif g != "" and g == q: #if the escd rxcui matches the dscd rxcui, rxcuis match
                                            incorrect_action1 = 0
                                            incorrect_action2 = 0
                                            incorrect_action3 = 0
                                            incorrect_action4 = 0
                                            match_details = "3"

                                        else:
                                            match_details = "1" #If none of the above matches, compare e to d Rx Ing, DF and Strength
                                            if d == '' or e == '' or f == '' or n == '' or o == '' or p == '':
                                                non_compounds.remove(item)
                                                continue
                                            else:
                                                # Check if any of the ingredients are in schedule_2_table
                                                ingredients = [d, h, n, r]
                                                if any(ingredient.lower() in schedule_2_table for ingredient in ingredients):
                                                    # If any ingredient is in schedule_2_ingredients, continue with exact matches
                                                    if (d.lower() == n.lower()) or (r != "" and d.lower() == r.lower()) or (r != "" and h.lower() == r.lower()) or (h != "" and h.lower() == n.lower()): #Compare erxNorm Ing to dRxNorm Ing, if not equal, then show 1 (wrong drug)
                                                        incorrect_action1 = 0 #if the ingredients match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                    else:
                                                        incorrect_action1 = 1
                                                        match_details = 1
                                                    if (p != "" and p is not None and f == p) or (t != "" and t is not None and f == t) or (t != "" and t is not None and j == t) or (j != "" and j == p): #If the erxnorm strength doesn't equal the drxnorm strength, then show 2 (wrong strength)
                                                        incorrect_action2 = 0 #if the strengths match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                    else:
                                                        incorrect_action2 = 1
                                                        match_details = 1
                                                    if  e == o or (e!= "" and e == s) or (i!= "" and i == s) or (i!= "" and i== o): #Compare erxnorm dose form to drxnorm dose form
                                                        incorrect_action3 = 0 #if the dose forms match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                    else:
                                                        incorrect_action3 = 1
                                                        match_details = 1
                                                    if incorrect_action1 == 0 and incorrect_action2 == 0 and incorrect_action3 == 0:
                                                        match_details = 2
                                                    filtered_for_redcap_withSCDS.append({
                                                        'record_id': item.get('uniqueID'),
                                                        'report_id': item.get('reportID'),
                                                        'redcap_data_access_group': redcap_group,
                                                        'report_row_number': item.get('rowID'),
                                                        'match_status': match_details,
                                                        'incorrect_action___1': incorrect_action1,
                                                        'incorrect_action___2': incorrect_action2,
                                                        'incorrect_action___3': incorrect_action3,
                                                        'incorrect_action___4': incorrect_action4,
                                                        'erx_ndc': item.get('eNDC'),
                                                        'erx_ingredient': (item.get('eRxNorm Ingredient')),
                                                        'erx_dose_form': (item.get('eRxNorm Dose Form')),
                                                        'erx_strength': (item.get('eRxNorm Strength Details')),
                                                        'medication_prescribed': item.get('original_erx_med_name'),
                                                        'medication_dispensed': item.get('pharm_Dispensed_med'),
                                                        'pharm_ndc': item.get('dNDC'),
                                                        'pharm_ingredient': (item.get('dRxNorm Ingredient')),
                                                        'pharm_dose_form': ((item.get('dRxNorm Dose Form'))).replace("-", ' '),
                                                        'pharm_strength': ((item.get('dRxNorm Strength Details'))).replace(',', ''),
                                                        'page_number': item.get('page_number')
                                                    })
                                                    # Continue to the next iteration of the loop
                                                    continue

                                                elif  (d.lower() == n.lower()) or (r != "" and d.lower() == r.lower()) or (r != "" and h.lower() == r.lower()) or (h != "" and h.lower() == n.lower()): #Compare erxNorm Ing to dRxNorm Ing, if not equal, then show 1 (wrong drug)
                                                    incorrect_action1 = 0 #if the ingredients match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                elif entries_are_true(d, n, ing_truth_table) or entries_are_true(d, r, ing_truth_table) or entries_are_true(h, r, ing_truth_table) or entries_are_true(h, n, ing_truth_table):
                                                    incorrect_action1 = 0
                                                elif (d.lower().startswith("influenza a virus") and n.lower().startswith("influenza a virus")) or (d.lower().startswith("influenza a virus") and r.lower().startswith("influenza a virus")) or\
                                                (h.lower().startswith("influenza a virus") and n.lower().startswith("influenza a virus")) or (h.lower().startswith("influenza a virus") and r.lower().startswith("influenza a virus")):
                                                    incorrect_action1 = 0
                                                else:
                                                    incorrect_action1 = 1 #if there is no ingredient match

                                                if  e == o or (e!= "" and e == s) or (i!= "" and i == s) or (i!= "" and i== o): #Compare erxnorm dose form to drxnorm dose form
                                                    incorrect_action3 = 0 #if the dose forms match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                elif entries_are_true(e, o, df_truth_table) or entries_are_true(i, s, df_truth_table) or entries_are_true(e, s, df_truth_table) or entries_are_true(i, o, df_truth_table):
                                                    incorrect_action3 = 0 # if the does forms are equivalent via the truth table, these dose form match
                                                elif check_entries_in_special_case_truth_table(e, o, special_case_truth_table) or check_entries_in_special_case_truth_table(i, s, special_case_truth_table) or check_entries_in_special_case_truth_table(i, o, special_case_truth_table) or check_entries_in_special_case_truth_table(e, s, special_case_truth_table):
                                                    incorrect_action3 = 0
                                                ###Account for encenteric coated aspirin
                                                elif (d.lower() == 'aspirin' and n.lower() == 'aspirin') or (d.lower() == 'aspirin' and r.lower() == 'aspirin') or (h.lower() == 'aspirin' and n.lower() == 'aspirin') or (h.lower() == 'aspirin' and r.lower() == 'aspirin'):
                                                    if (e == 'Oral Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (e == 'Oral Capsule, Oral Product, Pill' and o == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                    (e == 'Oral Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                    (e == 'Oral Capsule, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (e == 'Chewable Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (e == 'Oral Tablet, Oral Product, Pill' and s == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (e == 'Oral Capsule, Oral Product, Pill' and s == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                    (e == 'Oral Tablet, Oral Product, Pill' and s == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                    (e == 'Oral Capsule, Oral Product, Pill' and s == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (e == 'Chewable Tablet, Oral Product, Pill' and s == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (i == 'Oral Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (i == 'Oral Capsule, Oral Product, Pill' and o == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                    (i == 'Oral Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                    (i == 'Oral Capsule, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (i == 'Chewable Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (i == 'Oral Tablet, Oral Product, Pill' and s == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (i == 'Oral Capsule, Oral Product, Pill' and s == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                    (i == 'Oral Tablet, Oral Product, Pill' and s == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                    (i == 'Oral Capsule, Oral Product, Pill' and s == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                    (i == 'Chewable Tablet, Oral Product, Pill' and s == 'Delayed Release Oral Tablet, Oral Product, Pill'):
                                                        incorrect_action3 = 0
                                                elif (d.lower() == "ondansetron hydrochloride" and n.lower() == "ondansetron hydrochloride") or (d.lower() == 'ondansetron hydrochloride' and r.lower() == 'ondansetron hydrochloride') or (h.lower() == 'ondansetron hydrochloride' and n.lower() == 'ondansetron hydrochloride') or (h.lower() == 'ondansetron hydrochloride' and r.lower() == 'ondansetron hydrochloride'):
                                                    if (e == 'Oral Tablet, Oral Product, Pill' and o == 'Disintegrating Oral Tablet, Oral Product, Pill, Distintegrating Oral Product') or \
                                                        (e == 'Oral Tablet, Oral Product, Pill' and s == 'Disintegrating Oral Tablet, Oral Product, Pill, Distintegrating Oral Product') or \
                                                        (i == 'Oral Tablet, Oral Product, Pill' and o == 'Disintegrating Oral Tablet, Oral Product, Pill, Distintegrating Oral Product') or \
                                                        (i == 'Oral Tablet, Oral Product, Pill' and s == 'Disintegrating Oral Tablet, Oral Product, Pill, Distintegrating Oral Product'):
                                                        incorrect_action3 = 0
                                                #If the ingredient is one in the chewable_ing_truth_table and the dose form goes from Chewable Tablet to Oral Tablet, dose form is equivalent.
                                                elif d.lower() == n.lower() and d.lower() in chewable_ing_truth_table:
                                                    if (e == "Chewable Tablet" and o == "Oral Tablet") or (o == "Chewable Tablet" and e == "Oral Tablet"):
                                                        incorrect_action3 = 0
                                                elif d.lower() == r.lower() and d.lower() in chewable_ing_truth_table:
                                                    if (e == "Chewable Tablet" and s == "Oral Tablet") or (s == "Chewable Tablet" and e == "Oral Tablet"):
                                                        incorrect_action3 = 0
                                                elif h.lower() == n.lower() and h.lower() in chewable_ing_truth_table:
                                                    if (i == "Chewable Tablet" and o == "Oral Tablet") or (o == "Chewable Tablet" and i == "Oral Tablet"):
                                                        incorrect_action3 = 0
                                                elif h.lower() == r.lower() and h.lower() in chewable_ing_truth_table:
                                                    if (i == "Chewable Tablet" and s == "Oral Tablet") or (s == "Chewable Tablet" and i == "Oral Tablet"):
                                                        incorrect_action3 = 0
                                                else:
                                                    incorrect_action3 = 1 #if there is no dose form match

                                                if (p != "" and p is not None and f == p) or (t != "" and t is not None and f == t) or (t != "" and t is not None and j == t) or (j != "" and j == p): #If the erxnorm strength doesn't equal the drxnorm strength, then show 2 (wrong strength)
                                                    incorrect_action2 = 0 #if the strengths match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                elif (f != "" and f is not None and p != "" and p is not None) and compare_medication_strengths(f.lower(), p.lower()) is True:
                                                    incorrect_action2 = 0
                                                elif (f != "" and f is not None and t != "" and t is not None) and compare_medication_strengths(f.lower(), t.lower()) is True:
                                                    incorrect_action2 = 0
                                                elif (j != "" and j is not None and p != "" and p is not None) and compare_medication_strengths(j.lower(), p.lower()) is True:
                                                    incorrect_action2 = 0
                                                elif (j != "" and j is not None and t != "" and t is not None) and compare_medication_strengths(j.lower(), t.lower()) is True:
                                                    incorrect_action2 = 0
                                                elif (d.lower() == 'melatonin' and n.lower() == 'melatonin') or (d.lower() == 'melatonin' and r.lower() == 'melatonin') or (h.lower() == 'melatonin' and n.lower() == 'melatonin') or (h.lower() == 'melatonin' and r.lower() == 'melatonin'):
                                                    incorrect_action2 = 0
                                                else:
                                                    incorrect_action2 = 1 #if there is no strength match

                                                if incorrect_action1 == 0 and incorrect_action2 == 0 and incorrect_action3 == 0:
                                                    incorrect_action4 = 1 #All the components from RxNorm Match, but the RxCUIs do not
                                                    match_details = "2"

                                    elif "b" not in c.lower(): #IF the TTY is not branded
                                        match_details = "1" #If neither eNDC = dNDC or eRxCUI = dRxCUI and its not branded, compare e to d Rx Ing, DF and Strength
                                        if d == '' or e == '' or f == '' or n == '' or o == '' or p == '':
                                            non_compounds.remove(item)
                                            continue
                                        else:
                                            # Check if any of the ingredients are in schedule_2_table
                                            ingredients = [d, h, n, r]
                                            if any(ingredient.lower() in schedule_2_table for ingredient in ingredients):
                                                # If any ingredient is in schedule_2_ingredients, continue with exact matches
                                                if (d.lower() == n.lower()) or (r != "" and d.lower() == r.lower()) or (r != "" and h.lower() == r.lower()) or (h != "" and h.lower() == n.lower()): #Compare erxNorm Ing to dRxNorm Ing, if not equal, then show 1 (wrong drug)
                                                    incorrect_action1 = 0 #if the ingredients match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                else:
                                                    incorrect_action1 = 1
                                                    match_details = 1
                                                if (p != "" and p is not None and f == p) or (t != "" and t is not None and f == t) or (t != "" and t is not None and j == t) or (j != "" and j == p): #If the erxnorm strength doesn't equal the drxnorm strength, then show 2 (wrong strength)
                                                    incorrect_action2 = 0 #if the strengths match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                else:
                                                    incorrect_action2 = 1
                                                    match_details = 1
                                                if  e == o or (e!= "" and e == s) or (i!= "" and i == s) or (i!= "" and i== o): #Compare erxnorm dose form to drxnorm dose form
                                                    incorrect_action3 = 0 #if the dose forms match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                                else:
                                                    incorrect_action3 = 1
                                                    match_details = 1
                                                if incorrect_action1 == 0 and incorrect_action2 == 0 and incorrect_action3 == 0:
                                                    match_details = 2
                                                filtered_for_redcap_withSCDS.append({
                                                    'record_id': item.get('uniqueID'),
                                                    'report_id': item.get('reportID'),
                                                    'redcap_data_access_group': redcap_group,
                                                    'report_row_number': item.get('rowID'),
                                                    'match_status': match_details,
                                                    'incorrect_action___1': incorrect_action1,
                                                    'incorrect_action___2': incorrect_action2,
                                                    'incorrect_action___3': incorrect_action3,
                                                    'incorrect_action___4': incorrect_action4,
                                                    'erx_ndc': item.get('eNDC'),
                                                    'erx_ingredient': (item.get('eRxNorm Ingredient')),
                                                    'erx_dose_form': (item.get('eRxNorm Dose Form')),
                                                    'erx_strength': (item.get('eRxNorm Strength Details')),
                                                    'medication_prescribed': item.get('original_erx_med_name'),
                                                    'medication_dispensed': item.get('pharm_Dispensed_med'),
                                                    'pharm_ndc': item.get('dNDC'),
                                                    'pharm_ingredient': (item.get('dRxNorm Ingredient')),
                                                    'pharm_dose_form': ((item.get('dRxNorm Dose Form'))).replace("-", ' '),
                                                    'pharm_strength': ((item.get('dRxNorm Strength Details'))).replace(',', ''),
                                                    'page_number': item.get('page_number')
                                                })
                                                # Continue to the next iteration of the loop
                                                continue
                                            elif  (d.lower() == n.lower()) : #Compare erxNorm Ing to dRxNorm Ing, if not equal, then show 1 (wrong drug)
                                                incorrect_action1 = 0 #if the ingredients match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                            elif entries_are_true(d, n, ing_truth_table) or entries_are_true(d, r, ing_truth_table) or entries_are_true(h, r, ing_truth_table) or entries_are_true(h, n, ing_truth_table):
                                                incorrect_action1 = 0
                                            elif (d.lower().startswith("influenza a virus") and n.lower().startswith("influenza a virus")):
                                                incorrect_action1 = 0
                                            else:
                                                incorrect_action1 = 1 #if there is no ingredient match

                                            if  e == o: #Compare erxnorm dose form to drxnorm dose form
                                                incorrect_action3 = 0 #if the dose forms match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                            elif entries_are_true(e, o, df_truth_table) or entries_are_true(i, s, df_truth_table) or entries_are_true(e, s, df_truth_table) or entries_are_true(i, o, df_truth_table):
                                                incorrect_action3 = 0 # if the does forms are equivalent via the truth table, these dose form match
                                            elif check_entries_in_special_case_truth_table(e, o, special_case_truth_table) or check_entries_in_special_case_truth_table(i, s, special_case_truth_table) or check_entries_in_special_case_truth_table(i, o, special_case_truth_table) or check_entries_in_special_case_truth_table(e, s, special_case_truth_table):
                                                incorrect_action3 = 0
                                            ###Account for encenteric coated aspirin
                                            elif d.lower() == 'aspirin' and n.lower() == 'aspirin':
                                                if (e == 'Oral Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                (e == 'Oral Capsule, Oral Product, Pill' and o == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                (e == 'Oral Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Capsule, Oral Product, Pill') or \
                                                (e == 'Oral Capsule, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill') or \
                                                (e == 'Chewable Tablet, Oral Product, Pill' and o == 'Delayed Release Oral Tablet, Oral Product, Pill'):
                                                    incorrect_action3 = 0
                                            #If the ingredient is ondansetron hydrochloride and the doseform is prescribed as oral tablet, allow dispensed ondansetron hydrochloride and the dose form of disintegrating oral tablet, these are a match on dose form.
                                            elif d.lower() == "ondansetron hydrochloride" and n.lower() == "ondansetron hydrochloride":
                                                if (e == 'Oral Tablet, Oral Product, Pill' and o == 'Disintegrating Oral Tablet, Oral Product, Pill, Disintegrating Oral Product'):
                                                    incorrect_action3 = 0
                                                # else: 
                                                #     print (f"False, e = {e} and o ] {o}")
                                            #If the ingredient is one in the chewable_ing_truth_table and the dose form goes from Chewable Tablet to Oral Tablet, dose form is equivalent.
                                            elif d.lower() == n.lower() and d.lower() in chewable_ing_truth_table:
                                                if (e == "Chewable Tablet, Oral Product, Pill, Chewable Product" and o == "Oral Tablet, Oral Product, Pill") or (o == "Chewable Tablet, Oral Product, Pill, Chewable Product" and e == "Oral Tablet, Oral Product, Pill"):
                                                    incorrect_action3 = 0
                                            else:
                                                incorrect_action3 = 1 #if there is no dose form match

                                            if f.lower() == p.lower(): #Compare strengths
                                                incorrect_action2 = 0 #if the strengths match in any of the e/d esc/d, e/dscd, escd/dscd ways
                                            elif f != "" and f is not None and p != "N/A" and p is not None:
                                                if compare_medication_strengths(f.lower(), p.lower()) is True:
                                                    incorrect_action2 = 0
                                                else:
                                                    incorrect_action2 = 1
                                            elif d.lower() == 'melatonin' and n.lower() == 'melatonin':
                                                    incorrect_action2 = 0
                                            else:
                                                incorrect_action2 = 1 #if there is no strength match

                                            if incorrect_action1 == 0 and incorrect_action2 == 0 and incorrect_action3 == 0:
                                                incorrect_action4 = 1 #All the components from RxNorm Match, but the RxCUIs do not
                                                match_details = "2"


                                    ###Step 4: Create the list of items with necessary keys to go to REDCAP
                                    filtered_for_redcap_withSCDS.append({
                                            'record_id': item.get('uniqueID'),
                                            'report_id': item.get('reportID'),
                                            'redcap_data_access_group': redcap_group,
                                            'report_row_number': item.get('rowID'),
                                            'match_status': match_details,
                                            'incorrect_action___1': incorrect_action1,
                                            'incorrect_action___2': incorrect_action2,
                                            'incorrect_action___3': incorrect_action3,
                                            'incorrect_action___4': incorrect_action4,
                                            'erx_ndc': item.get('eNDC'),
                                            'erx_ingredient': (item.get('eRxNorm Ingredient')),
                                            'erx_dose_form': (item.get('eRxNorm Dose Form')),
                                            'erx_strength': (item.get('eRxNorm Strength Details')),
                                            'medication_prescribed': item.get('original_erx_med_name'),
                                            'medication_dispensed': item.get('pharm_Dispensed_med'),
                                            'pharm_ndc': item.get('dNDC'),
                                            'pharm_ingredient': (item.get('dRxNorm Ingredient')),
                                            'pharm_dose_form': ((item.get('dRxNorm Dose Form'))).replace("-", ' '),
                                            'pharm_strength': ((item.get('dRxNorm Strength Details'))).replace(',', ''),
                                            'page_number': item.get('page_number')
                                        })
                                pp.pprint(filtered_for_redcap_withSCDS[0:3])




                            print("Processing RxNorm information Complete! Onto uploading to Dropbox and Sending to Redcap")

                        except Exception as e:
                            print(f"Error processing {nfile.name}: {e}")

                        fieldnames = ['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4','erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength', 'page_number']

                        # Create a StringIO buffer to store the CSV data
                        csv_buffer = StringIO()
                        csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

                        # Write the CSV data to the buffer
                        csv_writer.writeheader()
                        csv_writer.writerows(filtered_for_redcap_withSCDS)

                        #Create the filename of the new csv file that will be uploaded to dropbox for later import into redcap.
                        new_name = f'{os.path.splitext(name)[0]}ForRedcap.csv'
                        #Set the Dropbox path where I want the completed file stored
                        upload_dropbox_path = f'{main_dropbox_folder_path}ReadyForRedcap/{new_name}'
                        #Upload the csv file to dropbos
                        dbx.files_upload(csv_buffer.getvalue().encode(), upload_dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
                        print("CSV for RedCap Done")

                         # Specify the Dropbox folder path you want to check or create
                        main_dropbox_folder_path = '/AHRQ_R18_Project_SAVERx/'
                        redcap_path = f'{main_dropbox_folder_path}ReadyForRedcap/'
                        try:
                            current_files = list_files_in_folder(redcap_path, dbx) #If there are files in any of the found folders, list what the files are as current files
                            #print(current_files)
                            for nfile in current_files: #process each file in current files one at a time
                                print(nfile)
                                name = nfile.split('.')[0]
                                #read in data from nfile to Process with RxNorm
                                dropbox_file_path = f"{redcap_path}{nfile}"
                                print(f"Dropbox File Path: {dropbox_file_path}")
                                #If the file is a .csv file, proceed this way:
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

                                for row in csv_reader:
                                    data.append(row)

                                mismatches = []
                                for item in data:
                                    if item['match_status'] == '1':
                                        mismatches.append(item)
                                print(len(mismatches))

                                #fieldnames = ['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4','erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength', 'prescribed qty', 'dispensed qty', 'page_number']
                                fieldnames = ['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4','erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength', 'page_number']

                                # Create a StringIO buffer to store the CSV data
                                csv_buffer = StringIO()
                                csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

                                # Write the CSV data to the buffer
                                csv_writer.writeheader()
                                csv_writer.writerows(mismatches)

                                #Create the filename of the new csv file that will be uploaded to dropbox for later import into redcap.
                                new_name = f'{os.path.splitext(name)[0]}Filtered.csv'
                                #Set the Dropbox path where I want the completed file stored
                                upload_dropbox_path = f'{main_dropbox_folder_path}ReadyForRedcap/{new_name}'
                                #Upload the csv file to dropbos
                                dbx.files_upload(csv_buffer.getvalue().encode(), upload_dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
                                print("CSV for RedCap Done")

                                #Delete the dropbox_file_path
                                try:
                                    # Assuming you've already set up the Dropbox client (dbx) and obtained the access token
                                    result = dbx.files_delete_v2(dropbox_file_path)
                                    print(f"File deleted: {result.metadata.name}")
                                except dropbox.exceptions.ApiError as err:
                                    print(f"Failed to delete file: {err}")


                        except:
                            print("Error")

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
    except AuthError as e:
        print("Error refreshing access token:", e)


if __name__ == "__main__":
    main()