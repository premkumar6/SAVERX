import pandas as pd
import csv
import pprint
import re
import numpy as np

#Set Pretty Print Rules to make the dictionaries look nice and easier to read when printed
pp = pprint.PrettyPrinter(indent=2, sort_dicts=False, width=100)


def read_excel_to_list_of_dicts(file_path, sheet_name=None):
    """
    Read an Excel (.xlsx) file into a list of dictionaries.

    Args:
        file_path (str): Path to the Excel file.
        sheet_name (str, optional): Name of the specific sheet to read. If None, the first sheet is used.

    Returns:
        list: A list of dictionaries where each dictionary represents a row in the Excel sheet.
    """
    try:
        if sheet_name is None:
            df = pd.read_excel(file_path)
            print(df.head())
        else:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as e:
        print("An error occurred while reading the Excel file:")
        print(e)

    # Convert the DataFrame to a list of dictionaries
    data_list = df.to_dict(orient='records')

    return data_list


def read_excel_to_dataframe(file_path, sheet_name=None):
    """
    Read an Excel (.xlsx) file into a DataFrame.

    Args:
        file_path (str): Path to the Excel file.
        sheet_name (str, optional): Name of the specific sheet to read. If None, the first sheet is used.

    Returns:
        pd.DataFrame: A DataFrame representing the Excel data.
    """
    if sheet_name is None:
        df = pd.read_excel(file_path)
        print(df.head())
    else:
        df = pd.read_excel(file_path, sheet_name=sheet_name)

    return df

def add_row_numbers(df):
    """
    Add a column with row numbers to a DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with a new 'RowNumber' column.
    """
    df.insert(0, 'record_id', range(2, len(df) + 2))
    return df


def remove_blank_columns_from_dataframe(df):
    """
    Remove completely blank columns from a DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with completely blank columns removed.
    """
    df = df.dropna(axis=1, how='all')
    return df

def parse_number(number_str):
    """
    Parse a formatted number string with commas and decimal points and return it as a whole number.

    Args:
        number_str (str): The formatted number string.

    Returns:
        int: The whole number without commas or decimal points.
    """

    #Ensure that the item provided really is a string so we can manipulate it
    number_str = str(number_str)
    # Remove commas from the number string
    if number_str != 'nan':
        number_str = number_str.replace(',', '')

    # Remove - from the number string
    if number_str!= 'nan':
        number_str = number_str.replace('-','')

    # Remove decimal point and everything after it
    if '.' in number_str:
        number_str = number_str.split('.')[0]

    # Convert the cleaned string to an integer
    try:
        return int(number_str)
    except ValueError:
        # Handle the case where the input string is not a valid number
        return number_str

def remove_nan(string_value):
    """
    Take a given string value, check to see if it contains 'nan' or is a NaN value.
    If it is, remove all occurrences of 'nan' and replace them with an empty string.

    Args:
        string_value (str or float): A string or float value.

    Returns:
        cleaned_value (str): A string with 'nan' occurrences removed.
    """
    print(f"Processing value: {string_value}")

    if isinstance(string_value, str) and string_value.lower() == 'nan':
        return ''
    elif isinstance(string_value, float) and np.isnan(string_value):
        return ''
    else:
        return str(string_value)

def condense_lines(list_of_dicts):
    """
    Take in a list of dictionaries and condense the information based on the key 'Dispensed NDC'.
    If a row has a 'Dispensed NDC' value and the next row does not, combine specific values
    for keys ('Escript prescribed item', 'Prescribed Item', and 'Dispensed Item') in the current and next rows
    (concatenate the strings).

    Args:
        list_of_dicts (List): A list of dictionaries where each dictionary maps to a row of data.

    Returns:
        list_of_dicts (list): A list of dictionaries where data is condensed as specified.
    """
    condensed_list = []  # Initialize a new list to store the condensed data
    i = 0

    while i < len(list_of_dicts):
        current_dict = list_of_dicts[i]

        if 'Dispensed NDC' in current_dict and current_dict['Dispensed NDC'] not in ['', 'nan']:
            # If 'Dispensed NDC' is present and not empty or 'nan'
            next_index = i + 1
            while next_index < len(list_of_dicts) and ('Dispensed NDC' not in list_of_dicts[next_index] or list_of_dicts[next_index]['Dispensed NDC'] in ['', 'nan']):
                # Concatenate values for specific keys
                for key in ['Escript prescribed item', 'Prescribed Item', 'Dispensed Item']:
                    string1 = str(current_dict.get(key, ''))
                    string2 = str(list_of_dicts[next_index].get(key, ''))
                    #current_dict[key] = current_dict.get(key, '') + ' ' + list_of_dicts[next_index].get(key, '')
                    current_dict[key] = string1 + ' ' + string2
                next_index += 1

            condensed_list.append(current_dict)
            i = next_index
        else:
            # If 'Dispensed NDC' is not present or is empty or 'nan', add the current row without changes
            condensed_list.append(current_dict)
            i += 1

    return condensed_list


def remove_nan(string_value):
    """
    Take a given string value, check to see if it contains 'nan' or is a NaN value.
    If it does, remove all occurrences of 'nan' and replace them with an empty string.
    Also, remove extra spaces.

    Args:
        string_value (str or float): A string or float value.

    Returns:
        cleaned_value (str): A string with 'nan' occurrences removed.
    """
    if isinstance(string_value, str):
        # Remove all 'nan' occurrences, extra spaces, and trim leading/trailing spaces
        cleaned_value = re.sub(r'\bnan\b', '', string_value).strip()
        cleaned_value = re.sub(r'\s+', ' ', cleaned_value).strip()
        return cleaned_value
    elif isinstance(string_value, float) and np.isnan(string_value):
        return ''
    else:
        return str(string_value)



def main_process(file):
    corrections = {'hydroxychloroquin e': 'hydroxychloroquine', 'dextroamphetamin e':'dextroamphetamine', 'hydrochlorothiazid e': 'hydrochlorothiazide', 'hydroclorothia zide': 'hydrochlorothiazide',
                   'medroxyprogester one': 'medroxyprogesterone', 'dextroamphet amine': 'dextroamphetamine', 'dexmethylphenidat e': 'dexmethylphenidate', 'dexmethylphenida te': 'dexmethylphenidate',
                    'methylprednisol one': 'methylprednisolone'}

    # input_file_path = file
    # name = input_file_path.split('/')[-1]
    # name = name.split('.')[0]
    # folder = input_file_path.split('/')[-2]
    # print(name)
    # # Read the Excel file into a DataFrame
    # df = read_excel_to_dataframe(input_file_path)
    # print(df.head())

    df = pd.DataFrame(file)

    # Add row numbers to the DataFrame
    df = add_row_numbers(df)

    # Remove completely blank columns
    df = remove_blank_columns_from_dataframe(df)

    # Convert the DataFrame to a list of dictionaries
    raw_data = df.to_dict(orient='records')

    for row in raw_data:
        row['Escript NDC'] = parse_number(row.get('Escript NDC'))
        row['Prescribed NDC'] = parse_number(row.get('Prescribed NDC'))
        row['Dispensed NDC'] = parse_number(row.get('Dispensed NDC'))
        row['GCN'] = parse_number(row.get('GCN'))

    # print(len(raw_data))
    # pp.pprint(raw_data[0:10])

    count_dNDC = 0
    count_dDescriptions = 0
    for row in raw_data:
        if row.get('Dispensed NDC') != "" and row.get('Dispensed NDC') != "nan":
            count_dNDC += 1
            ditem = row.get('Dispensed Item')
            if not ditem == "" or not ditem == "nan":
                count_dDescriptions += 1
            else:
                count_dDescriptions += 0
        else:
            count_dNDC += 0

    print(f"Number of dNDCs: {count_dNDC} and Number of Dispensed Items: {count_dDescriptions}")

    cleaned_data = condense_lines(raw_data)
    # pp.pprint(cleaned_data[0:10])

    for row in cleaned_data:
        for key, value in row.items():
            row[key] = remove_nan(value)
        # print(row)

    new_cleaned_data = []

    for row in cleaned_data:
        if row.get('Escript prescribed item') != '' or row.get('Escript NDC') != '' or row.get('Prescribed Item') != "" or row.get('Prescribed NDC') != "" or row.get('Dispensed Item') != "" or row.get('Dispensed NDC') != "":
            new_cleaned_data.append(row)

    # print(len(new_cleaned_data))
    # pp.pprint(new_cleaned_data[0:10])

    for row in new_cleaned_data:
        if 'Escript prescribed item' in row:
            item = row['Escript prescribed item'].lower()
            for key, value in corrections.items():
                item = item.replace(key, value)
            row['Escript prescribed item'] = item
        if 'Prescribed Item' in row:
            item = row['Prescribed Item'].lower()
            for key, value in corrections.items():
                item = item.replace(key, value)
            row['Prescribed Item'] = item
        if 'Dispensed Item' in row:
            item = row['Dispensed Item'].lower()
            for key, value in corrections.items():
                item = item.replace(key, value)
            row['Dispensed Item'] = item

    for row in new_cleaned_data:
        for key, value in row.items():
            if isinstance(value, str):  # Check if the value is a string
                # Remove spaces after hyphens and forward slashes and before percents
                value = value.replace('- ', '-').replace('/ ', '/').replace(' %', "%")
                row[key] = value

    # pp.pprint(new_cleaned_data[0:10])

    # myFile1 = open(f'/Users/meganwhitaker/Documents/Megan/umich/SaveRx/{folder}/Formatted{name}.csv', 'w')
    # writer = csv.writer(myFile1)
    # writer.writerow(['rowID', 'Escript prescribed item', 'Escript NDC', 'Prescribed Item', 'Prescribed NDC', 'Dispensed Item', 'Dispensed NDC', 'GCN'])
    # for data in new_cleaned_data:
    #     writer.writerow(data.values())
    # myFile1.close()

    # print("Done")

    return new_cleaned_data


if __name__ == "__main__":
    main_process()


