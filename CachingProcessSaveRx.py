import requests
import csv
import pprint
from csv import DictWriter
from timeit import default_timer as timer
import pandas as pd
import os
from collections import defaultdict, OrderedDict
from time import sleep
import time

#Add Natural Language Processing Info Specific to Med12)
# nlp = spacy.load("en_med12_trf")
#Set Pretty Print Rules to make the dictionaries look nice and easier to read when printed
pp = pprint.PrettyPrinter(indent=2, sort_dicts=False, width=100)

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
                        rxcui = data["ndcStatus"]["rxcui"]
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
                        rxcui = data["ndcStatus"]["rxcui"]
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

def main_process(file, redcap_process, name, rxcui_data):
# ############################################################################################################################################################################################################
    ###This section of code gets the RxCuis' from the e and dscript NDCs###
    brenmo_data = file
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
            if entry.get('Escript NDC') != '':
                e_rxcui = fetch_data_from_api(entry.get('Escript NDC'), count)
            else:
                e_rxcui = ""
            # if entry.get('Prescribed NDC') != '':
            #     p_rxcui = fetch_data_from_api(entry.get('Prescribed NDC'), count)
            # else:
            #     p_rxcui = ""
            if entry.get('Dispensed NDC') != '':
                d_rxcui = fetch_data_from_api(entry.get('Dispensed NDC'),count)
            else:
                d_rxcui = ""
            entry['e_rxcui'] = e_rxcui
            # entry['p_rxcui'] = p_rxcui
            entry['d_rxcui'] = d_rxcui
        except Exception as e:
           error_message = (f"An error occured for NDC {entry['Escript NDC']} : {str(e)}")
           print(error_message)
           error_messages.append(error_message)
           continue
        new_dict = {
            'uniqueID': f"{name}{entry.get('rowID')}",
            'rowID': entry.get('rowID'),
            'reportID': name,
            'Escript prescribed item': entry.get('Escript prescribed item'),
            'Escript NDC': entry.get('Escript NDC'),
            # 'Prescribed Item': entry.get('Prescribed Item'),
            # 'Prescribed NDC': entry.get('Prescribed NDC'),
            'Dispensed Item': entry.get('Dispensed Item'),
            'Dispensed NDC': entry.get('Dispensed NDC'),
            'GCN': entry.get('GCN'),
            'E_rxcui': e_rxcui,
            # 'P_rxcui': p_rxcui,
            'D_rxcui': d_rxcui,
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
    # myFile = open(f'{name}_with_RxCUI.csv', 'w')
    # writer = csv.writer(myFile)
    # writer.writerow(['uniqueID', 'record_id', 'reportID', 'Escript prescribed item', 'Escript NDC', 'Prescribed Item', 'Prescribed NDC','Dispensed Item', 'Dispensed NDC', 'GCN', 'E_rxcui', 'P_rxcui', 'D_rxcui'])
    # for data in addition:
    #     writer.writerow(data.values())
    # myFile.close()
    print("Compiled Data with RxCuis DONE")

# ###########################################################################################################
###This section adds the RxNorm Details about the Ingredients, Strengths, and Dose Forms###
    #rxcui_data = read_csv_to_dicts(f'{directory}compiled_data_withRxCUI.csv')
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
                    # e_info.append(matching_items)
                    # e_matching_info.extend(matching_items)
                    # print(e_matching_info)
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

                # #get the prescribed drug name and tty and RxNorm Drug Information
                # e_info = fetch_rxnorm_data_from_api(eRxCUI) #This is a dictionary with name and tty
                # #note the url in case I have any questions about why something didn't work
                # url_rxcui = f'https://rxnav.nlm.nih.gov/REST/rxcui/{eRxCUI}/historystatus.json'
                # # print(f"eRxCUI is {url_rxcui}")
                # # pp.pprint(f"eRxCui_info: {count} is {e_info}")
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

            # #get the rxcui from the dispensed medication and use tht to get the drug info from rxnorm
            # dRxCUI = item.get('D_rxcui')
            # #get the dispensed drug name and tty
            # d_info = fetch_rxnorm_data_from_api(dRxCUI) #This is a dictionary with name and tty
            # #note the url in case I have any questions about why something didn't work
            # url_rxcui = f'https://rxnav.nlm.nih.gov/REST/rxcui/{dRxCUI}/historystatus.json'
            # # print(f"dRxCUI is {url_rxcui}")
            # # pp.pprint(f"dRxCui_info: {count} is {d_info}")
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
            "eNDC": item.get('Escript NDC'),
            "eRxCUI": item.get('E_rxcui'),
            "original_erx_med_name": item.get('Escript prescribed item'),
            "dNDC": item.get("Dispensed NDC"),
            "dRxCUI": item.get('D_rxcui'),
            "pharm_Dispensed_med": item.get('Dispensed Item'),
            "GCN": item.get('GCN'),
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
            "dSCD RxNorm Strength Details": d_info["C RxNorm Strength Details"]
        }) #32 keys

    #Record the stop time for this process
    stop_time2 = time.time()
    pp.pprint(added_rxnorm)
    print("Finished Getting RxCUIs")
    elapsed_time2 = stop_time2 - start_time2
    print(f"The Time it took to get {len(addition)} rxnorm info is {elapsed_time2}")
    # pp.pprint(added_rxnorm)

    # myFile = open(f'{name}RxNorm_data.csv', 'w')
    # writer = csv.writer(myFile)
    # writer.writerow(['uniqueID', 'rowID', 'reportID', 'eNDC', 'eRxCUI', 'original_erx_med_name','dNDC', 'dRxCUI', 'pharm_Dispensed_med',
    #                    'GCN', "eRxNorm Name", 'eRxNorm TTY', 'eRxNorm Ingredient', 'eRxNorm Dose Form', 'eRxNorm Strength Details', "eSCD RxNorm Name",
    #                    "eSCD RxNorm TTY", "eSCD RxCUI", "eSCD RxNorm Ingredient", "eSCD RxNorm Dose Form", 'eSC DRxNorm Strength Detail',
    #                     "dRxNorm Name", "dRxNorm TTY", "dRxNorm Ingredient", "dRxNorm Dose Form", "dRxNorm Strength Details", "dSCD RxNorm Name",
    #                   "dSCD RxNorm TTY", "dSCD RxCUI", "dSCD RxNorm Ingredient", "dSCD RxNorm Dose Form", "dSCD RxNorm Strength Details"])

    # for data in added_rxnorm:
    #     writer.writerow(data.values())
    # myFile.close()
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

    #myFile = open('/Users/meganwhitaker/Documents/Megan/umich/SaveRx/TestingFolder/Fitchburg_completed_data.csv', 'w', encoding='utf-8')
    # myFile = open(f'{directory}completed_data.csv', 'w', encoding='utf-8')
    # writer = csv.writer(myFile)
    # writer.writerow(['uniqueID', 'rowID', 'reportID', 'eNDC', 'eRxCUI', 'original_erx_med_name','dNDC', 'dRxCUI', 'pharm_Dispensed_med',
    #                   'GCN', 'Escript Match Prescribed?', "eRxNorm Name", 'eRxNorm TTY','eRxNorm Ingredient', 'eRxNorm Dose Form',
    #                   'eRxNorm Strength Details', "eSCD RxNorm Name", "eSCD RxNorm TTY", "eSCD RxCUI", "eSCD RxNorm Ingredient",
    #                   "eSCD RxNorm Dose Form", 'eSCD RxNorm Strength Detail', "dRxNorm Name", "dRxNorm TTY", "dRxNorm Ingredient",
    #                   "dRxNorm Dose Form", "dRxNorm Strength Details", "dSCD RxNorm Name", "dSCD RxNorm TTY", "dSCD RxCUI",
    #                   "dSCD RxNorm Ingredient", "dSCD RxNorm Dose Form", "dSCD RxNorm Strength Details"])
    # for item in added_rxnorm:
    #     writer.writerow(item.values())
    # myFile.close()
    # print("CSV DONE")


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
        if item.get('Dispensed Item') is not None:
            if "cmpd" in item.get('Dispensed Item').lower() and item.get('Dispensed NDC') is None:
                compounds.append(item)
    print(len(compounds))

    non_compounds = [item for item in has_eNDCtoo if item not in compounds]

    ### Step 2.5 Add truth table of values for equivalent dose forms, and salt forms
    ing_truth_table = [
        {"hydroxyzine pamoate": "hydroxyzine hydrochloride"},
        {"doxycycline hyclate": "doxycycline monohydrate"},
        {"desvenlafaxine": "devenlafaxine succinate"}
    ]

    df_truth_table = [
        {"Auto-Injector, Injectable Product": "Cartridge, Injectable Product"},
        {"Delayed Release Oral Tablet, Oral Product, Pill": "Delayed Release Oral Capsule, Oral Product, Pill"},
        {"Extended Release Oral Tablet, Oral Product, Pill": "Extended Release Oral Capsule, Oral Product, Pill"},
        {"Injection, Injectable Product": "Prefilled Syringe, Injectable Product"},
        {"Injection, Injectable Product": "Prefilled Syringe, Injectable Product"},
        {"Prefilled Syringe, Injectable Product": "Cartridge, Injectable Product"},
        {"Injection, Injectable Product": "Cartridge, Injectable Product"},
        {"Injectable Solution, Injectable Product": "Pen Injector, Injectable Product"},
        {"Injectable Solution, Injectable Product": "Injection, Injectable Product"},
        {"Injectable Solution, Injectable Product": "Prefilled Syringe, Injectable Product"},
        {"Injectable Suspension, Injectable Product": "Injection, Injectable Product"},
        {"Ophthalmic Solution, Ophthalmic Product": "Ophthalmic Gel, Ophthalmic Product"},
        {"Injectable Suspension, Injectable Product": "Prefilled Suspension, Injectable Product"},
        {"Oral Capsule, Oral Product, Oral Pill": "Oral Tablet, Oral Product, Pill"},
        {"Oral Capsule, Oral Product, Oral Pill": "Pill"},
        {"Oral Solution, Oral Product, Oral Liquid Product": "Oral Suspension, Oral Product, Oral Liquid Product"},
        {"Oral Tablet, Oral Product, Pill": "Pill"},
        {"Oral Tablet, Oral Product, Pill": "Pack, Oral Product, Pill"},
        {"Oral Tablet, Oral Product, Pill": "Oral Capsule, Oral Product, Pill"},
        {"Otic Solution, Otic Product": "Otic Suspension, Otic Product"},
        {"Pack, Topical Product, Transdermal Product": "Transdermal System, Topical Product, Transdermal Product"},
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
                    if  (d.lower() == n.lower()) or (r != "" and d.lower() == r.lower()) or (r != "" and h.lower() == r.lower()) or (h != "" and h.lower() == n.lower()): #Compare erxNorm Ing to dRxNorm Ing, if not equal, then show 1 (wrong drug)
                        incorrect_action1 = 0 #if the ingredients match in any of the e/d esc/d, e/dscd, escd/dscd ways
                    elif entries_are_true(d, n, ing_truth_table) or entries_are_true(d, r, ing_truth_table) or entries_are_true(h, r, ing_truth_table) or entries_are_true(h, n, ing_truth_table):
                        incorrect_action1 = 0
                    else:
                        incorrect_action1 = 1 #if there is no ingredient match

                    if  e == o or (e!= "" and e == s) or (i!= "" and i == s) or (i!= "" and i== o): #Compare erxnorm dose form to drxnorm dose form
                        incorrect_action3 = 0 #if the dose forms match in any of the e/d esc/d, e/dscd, escd/dscd ways
                    elif entries_are_true(e, o, df_truth_table) or entries_are_true(i, s, df_truth_table) or entries_are_true(e, s, df_truth_table) or entries_are_true(i, o, df_truth_table):
                        incorrect_action3 = 0 # if the does forms are equivalent via the truth table, these dose form match
                    elif check_entries_in_special_case_truth_table(e, o, special_case_truth_table) or check_entries_in_special_case_truth_table(i, s, special_case_truth_table) or check_entries_in_special_case_truth_table(i, o, special_case_truth_table) or check_entries_in_special_case_truth_table(e, s, special_case_truth_table):
                        incorrect_action3 = 0
                    else:
                        incorrect_action3 = 1 #if there is no dose form match

                    if f.lower() == p.lower() or (t != "" and t is not None and f == t) or (t != "" and t is not None and j == t) or (j != "" and j == p): #If the erxnorm strength doesn't equal the drxnorm strength, then show 2 (wrong strength)
                        incorrect_action2 = 0 #if the strengths match in any of the e/d esc/d, e/dscd, escd/dscd ways
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
                if  (d.lower() == n.lower()) : #Compare erxNorm Ing to dRxNorm Ing, if not equal, then show 1 (wrong drug)
                    incorrect_action1 = 0 #if the ingredients match in any of the e/d esc/d, e/dscd, escd/dscd ways
                elif entries_are_true(d, n, ing_truth_table) or entries_are_true(d, r, ing_truth_table) or entries_are_true(h, r, ing_truth_table) or entries_are_true(h, n, ing_truth_table):
                    incorrect_action1 = 0
                else:
                    incorrect_action1 = 1 #if there is no ingredient match

                if  e == o: #Compare erxnorm dose form to drxnorm dose form
                    incorrect_action3 = 0 #if the dose forms match in any of the e/d esc/d, e/dscd, escd/dscd ways
                elif entries_are_true(e, o, df_truth_table) or entries_are_true(i, s, df_truth_table) or entries_are_true(e, s, df_truth_table) or entries_are_true(i, o, df_truth_table):
                    incorrect_action3 = 0 # if the does forms are equivalent via the truth table, these dose form match
                elif check_entries_in_special_case_truth_table(e, o, special_case_truth_table) or check_entries_in_special_case_truth_table(i, s, special_case_truth_table) or check_entries_in_special_case_truth_table(i, o, special_case_truth_table) or check_entries_in_special_case_truth_table(e, s, special_case_truth_table):
                    incorrect_action3 = 0
                else:
                    incorrect_action3 = 1 #if there is no dose form match

                if f.lower() == p.lower(): #If the erxnorm strength doesn't equal the drxnorm strength, then show 2 (wrong strength)
                    incorrect_action2 = 0 #if the strengths match in any of the e/d esc/d, e/dscd, escd/dscd ways
                else:
                    incorrect_action2 = 1 #if there is no strength match

                if incorrect_action1 == 0 and incorrect_action2 == 0 and incorrect_action3 == 0:
                        incorrect_action4 = 1 #All the components from RxNorm Match, but the RxCUIs do not
                        match_details = "2"

        ###Step 4: Create the list of items with necessary keys to go to REDCAP
        filtered_for_redcap_withSCDS.append({
                'record_id': item.get('uniqueID'),
                'report_id': item.get('reportID'),
                'redcap_data_access_group': redcap_process,
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
             })
    pp.pprint(filtered_for_redcap_withSCDS[0:3])

    ###Step 5: Write the CSV
    # Remove the ".csv" extension and add the desired suffix
    # new_name = f'{os.path.splitext(name)[0]}ForRedcap.csv'
    # myFile4 = open(new_name, 'w', encoding='utf-8', newline='')
    # writer = csv.writer(myFile4)
    # writer.writerow(['record_id', 'report_id', 'redcap_data_access_group', 'report_row_number', 'match_status', 'incorrect_action___1', 'incorrect_action___2','incorrect_action___3','incorrect_action___4',
    #                  'erx_ndc', 'erx_ingredient', 'erx_dose_form', 'erx_strength', 'medication_prescribed', 'medication_dispensed', 'pharm_ndc', 'pharm_ingredient', 'pharm_dose_form', 'pharm_strength'])
    # for data in filtered_for_redcap_withSCDS:
    #     writer.writerow(data.values())
    # myFile4.close()
    # print("CSV for RedCap Done")

    # return filtered_for_redcap_withSCDS
    print(type(filtered_for_redcap_withSCDS))
    print(type(error_messages))
    print(type(second_set_error_messages))
    return filtered_for_redcap_withSCDS, error_messages, second_set_error_messages, new_rxcui_data

if __name__ == "__main__":
    main_process()