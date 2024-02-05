import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import tabula
import datetime
import pandas as pd
import re
from helper_functions_ea import Logger
from india_mopng_etl.utils.base_classes import DataExtractor


def get_pdf_lists(url):
    read = requests.get(url)
    html_content = read.content
    soup = BeautifulSoup(html_content, "html.parser")

    # created an empty list for putting the pdfs
    list_of_pdf = []

    # accessed all the anchors tags
    anchor_tags = soup.find_all('a')

    # iterate through anchor tags for checking the extension is .pdf
    for link in anchor_tags:
        if link.get('href').endswith('.pdf'):
            if not link.get('href').startswith('http'):
                list_of_pdf.append(urljoin(url, link.get('href')))
            else:
                # Adding the pdf links into the list_of_pdf
                list_of_pdf.append(link.get('href'))
    return list_of_pdf
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/mprdec2017_revised.pdf']
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/mprsep2023.pdf']
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-for-Feb22.pdf']


def month_year_by_table_header(full_string, substring):
    # Find the starting index of the substring
    start_index = full_string.find(substring)
    # Extract the part of the string after the substring
    extracted_string = full_string[start_index + len(substring):]
    # Split the extracted string by the word "and" to get the required part
    required_string = extracted_string.split("and")[0].strip()
    return required_string


def format_to_datetime_object_by_string(date_string):
    # Assuming the day is the first day of the month
    day = 1
    # Convert the string to a datetime object
    datetime_object = datetime.datetime.strptime(date_string, "%B, %Y")
    # Set the day value
    datetime_object = datetime_object.replace(day=day)
    return datetime_object


def remove_special_and_numeric_characters(input_string):
    # Define regular expression pattern to match non-alphanumeric characters and whitespace
    pattern = r'[^a-zA-Z0-9\s]'
    # Replace non-alphanumeric characters and whitespace with an empty string
    clean_string = re.sub(pattern, '', input_string)
    return clean_string


def get_pdf_info(pdf_path, dataframes, main_variables):
    tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, lattice=True)

    target_header_for_crude = "Crude Oil Production during the month of"
    target_header_for_petroleum = "Production of Petroleum Products during the month of"
    crude_target_table = None
    petroleum_target_table = None

    for table in tables:
        # Checking the Crude header is present or not
        if table.shape[0] > 0 and pd.notnull(table.iloc[0, 0]) and target_header_for_crude in str(table.iloc[0, 0]):
            crude_target_table = table
            crude_target_table_header = str(table.iloc[0, 0])
        # elif any(target_header_for_crude in header for header in table.columns):
        #     crude_target_table = table
        #     for header in table.columns:
        #         if "Annexure-I" in header:
        #             crude_target_table_header = header.split('\r')[1].strip()
        #             break
        # else:
        #     pass

        # Checking the Petroleum header is present or not
        if table.shape[0] > 0 and pd.notnull(table.iloc[0, 0]) and target_header_for_petroleum in str(
                table.iloc[0, 0]):
            petroleum_target_table = table
            petroleum_target_table_header = str(table.iloc[0, 0])
        # elif any(target_header_for_petroleum in header for header in table.columns):
        #     petroleum_target_table = table
        #     for header in table.columns:
        #         if "Annexure-V" in header:
        #             petroleum_target_table_header = header.split('\r')[1].strip()
        #             break
        # petroleum_target_table_header = next(
        #     (header for header in table.columns if target_header_for_petroleum in header), None)
        # else:
        #     pass
        if crude_target_table is not None and petroleum_target_table is not None:
            break

    if crude_target_table is not None and petroleum_target_table is not None:
        crude_month_year_string = month_year_by_table_header(crude_target_table_header, target_header_for_crude)
        crude_month_year_datetime_object = format_to_datetime_object_by_string(crude_month_year_string)

        petroleum_month_year_string = month_year_by_table_header(petroleum_target_table_header,
                                                                 target_header_for_petroleum)
        petroleum_month_year_datetime_object = format_to_datetime_object_by_string(petroleum_month_year_string)

        if crude_target_table is not None:
            break_point = None
            for i, row in enumerate(crude_target_table.values):
                for cell in row:
                    if "(1)" in str(cell):
                        break_point = i
                        break
            if break_point is not None:
                # Remove the upper rows from where main data are coming
                crude_target_table = crude_target_table.iloc[break_point + 1:, :].reset_index(drop=True)
                # Remove the last row
                crude_target_table = crude_target_table.iloc[:-1]

            if crude_target_table.shape[1] == 11:
                # Select only the first and fifth columns for every rows
                crude_target_table = crude_target_table.iloc[:, [0, 4]]
                parent_text = ""
                for row_index, row in crude_target_table.iterrows():
                    if crude_target_table.iloc[[row_index]].iloc[0, 0].split(". ")[0].lower() in main_variables[
                        'crude_main_variables']:
                        parent_text = crude_target_table.iloc[[row_index]].iloc[0, 0].split(". ")[0].lower()
                    elif 'grand' in crude_target_table.iloc[[row_index]].iloc[0, 0].split(". ")[0].lower():
                        parent_text = 'grand'
                    if parent_text == '1' and 'ongc' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc'].append(crude_table_with_required_columns)
                    elif parent_text == '1' and 'onshore' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_onshore'].append(crude_table_with_required_columns)
                    elif parent_text == '1' and 'andhra' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_onshore_andhra_pradesh'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '1' and 'assam' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_onshore_assam'].append(crude_table_with_required_columns)
                    elif parent_text == '1' and 'gujarat' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_onshore_gujarat'].append(crude_table_with_required_columns)
                    elif parent_text == '1' and 'tamil' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_onshore_tamil_nadu'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '1' and 'tripura' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_onshore_tripura'].append(crude_table_with_required_columns)
                    elif parent_text == '1' and 'offshore' in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower() and 'eastern' not in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower() and 'western' not in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_offshore'].append(crude_table_with_required_columns)
                    elif parent_text == '1' and 'eastern' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_offshore_eastern_offshore'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '1' and 'western' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_offshore_western_offshore'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '1' and 'condensate' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['ongc_offshore_condensates'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '2' and 'oil' in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower() and '2' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['oil_onshore'].append(crude_table_with_required_columns)
                    elif parent_text == '2' and 'assam' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['oil_onshore_assam'].append(crude_table_with_required_columns)
                    elif parent_text == '2' and 'arunachal' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['oil_onshore_arunachal_pradesh'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '2' and (
                            'rajasthan' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower() or 'rajsthan' in
                            crude_target_table.iloc[[row_index]].iloc[0, 0].lower()):
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['oil_onshore_rajasthan'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'private' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private'].append(crude_table_with_required_columns)
                    elif parent_text == '3' and 'onshore' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_onshore'].append(crude_table_with_required_columns)
                    elif parent_text == '3' and 'andhra' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_onshore_andhra_pradesh'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'arunachal' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_onshore_arunachal_pradesh'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'assam' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_onshore_assam'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'gujarat' in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower() and 'offshore' not in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_onshore_gujarat'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'rajasthan' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_onshore_rajasthan'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'tamil' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_onshore_tamil_nadu'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'bengal' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_onshore_west_bengal'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'offshore' in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower() and 'eastern' not in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower() and 'western' not in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower() and 'gujarat' not in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_offshore'].append(crude_table_with_required_columns)
                    elif parent_text == '3' and 'eastern' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_offshore_eastern_offshore'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'gujarat' in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower() and 'offshore' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_offshore_gujarat_offshore'].append(
                            crude_table_with_required_columns)
                    elif parent_text == '3' and 'western' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['private_offshore_western_offshore'].append(
                            crude_table_with_required_columns)
                    elif parent_text == 'grand' and 'total' in crude_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['grand_total'].append(crude_table_with_required_columns)
                    elif parent_text == 'grand' and 'onshore' in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['grand_total_onshore'].append(crude_table_with_required_columns)
                    elif parent_text == 'grand' and 'offshore' in crude_target_table.iloc[[row_index]].iloc[
                        0, 0].lower():
                        crude_table_with_required_columns = crude_target_table.iloc[[row_index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes']['grand_total_offshore'].append(crude_table_with_required_columns)
                    else:
                        pass
        if petroleum_target_table is not None:
            break_point = None
            for i, row in enumerate(petroleum_target_table.values):
                for cell in row:
                    if "(1)" in str(cell):
                        break_point = i
                        break
            if break_point is not None:
                # Remove the upper rows from where main data are coming
                petroleum_target_table = petroleum_target_table.iloc[break_point + 1:, :].reset_index(drop=True)
                # Remove the last row
                petroleum_target_table = petroleum_target_table.iloc[:-1]

            if petroleum_target_table.shape[1] == 11:
                # Select only the first and fifth columns for every rows
                petroleum_target_table = petroleum_target_table.iloc[:, [0, 4]]
                for row_index, row in petroleum_target_table.iterrows():
                    if 'cpse' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() or 'public' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['cpse'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'guwahati' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_guwahati'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'barauni' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_barauni'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'gujarat' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_gujarat'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'haldia' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_haldia'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'mathura' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_mathura'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'digboi' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_digboi'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'panipat' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_panipat'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'bongaigaon' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_bongaigaon'].append(
                            petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'paradip' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ioc_paradip'].append(petroleum_table_with_required_columns)
                    elif 'ioc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'total' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['total_ioc'].append(petroleum_table_with_required_columns)
                    elif 'bpcl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'mumbai' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['bpcl_mumbai'].append(petroleum_table_with_required_columns)
                    elif 'bpcl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'kochi' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['bpcl_kochi'].append(petroleum_table_with_required_columns)
                    elif 'bpcl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'bina' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['bpcl_bina'].append(petroleum_table_with_required_columns)
                    elif 'bpcl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'total' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['total_bpcl'].append(petroleum_table_with_required_columns)
                    elif 'hpcl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'mumbai' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['hpcl_mumbai'].append(petroleum_table_with_required_columns)
                    elif 'hpcl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'visakh' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['hpcl_visakh'].append(petroleum_table_with_required_columns)
                    elif 'hpcl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'total' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['total_hpcl'].append(petroleum_table_with_required_columns)
                    elif 'cpcl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'manali' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['cpcl_manali'].append(petroleum_table_with_required_columns)
                    elif 'nrl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'numaligarh' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['nrl_numaligarh'].append(
                            petroleum_table_with_required_columns)
                    elif 'mrpl' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'mangalore' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['mrpl_mangalore'].append(
                            petroleum_table_with_required_columns)
                    elif 'ongc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'tatipaka' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ongc_tatipaka'].append(
                            petroleum_table_with_required_columns)
                    elif 'private' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['private'].append(petroleum_table_with_required_columns)
                    elif 'hmel' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'ggsr' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'bhatinda' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['hmel_ggsr_bhatinda'].append(
                            petroleum_table_with_required_columns)
                    elif 'ril' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'jamnagar' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ril_jamnagar'].append(petroleum_table_with_required_columns)
                    elif 'ril' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'sez' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ril_sez'].append(petroleum_table_with_required_columns)
                    elif 'ril' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'total' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['total_ril'].append(petroleum_table_with_required_columns)
                    elif 'nel' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'vadinar' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['nel_vadinar'].append(petroleum_table_with_required_columns)
                    elif 'refinery' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'total' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['total_refinery'].append(
                            petroleum_table_with_required_columns)
                    elif 'ongc' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'tatipaka' not in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['ongc'].append(petroleum_table_with_required_columns)
                    elif 'gail' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['gail'].append(petroleum_table_with_required_columns)
                    elif 'oil' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['oil'].append(petroleum_table_with_required_columns)
                    elif 'fractionator' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'total' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['total_fractionator'].append(
                            petroleum_table_with_required_columns)
                    elif 'grand' in petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower() and 'total' in \
                            petroleum_target_table.iloc[[row_index]].iloc[0, 0].lower():
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[row_index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes']['grand_total'].append(petroleum_table_with_required_columns)
                    else:
                        pass
                # if len(petroleum_target_table) >= len(dataframes['petroleum_dataframes']):
                #     for index, key in enumerate(dataframes['petroleum_dataframes']):
                #         petroleum_table_with_required_columns = petroleum_target_table.iloc[[index]]
                #         petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                #             petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                #         dataframes['petroleum_dataframes'][key].append(petroleum_table_with_required_columns)
    else:
        print("Target header not found in any table.")
    return dataframes


class Scrapper(DataExtractor):  # make sure you rename the class to your preference
    """Make sure you implement all the methods required for your ETL"""

    logger = Logger("India Mopng Scrape => ").logger

    # name = "India Mopng Scrape ETL"

    def __init__(self, website_url, dataframes, main_variables):
        self.list_of_pdfs = []
        self.crude_df = pd.DataFrame()
        self.petroleum_df = pd.DataFrame()
        self.website_url = website_url
        self.dataframes = dataframes
        self.main_variables = main_variables

    def get_list_of_pdfs(self):
        """
        Gets the PDF links from the website
        """
        self.logger.info("Getting List of PDFs")
        try:
            self.list_of_pdfs = get_pdf_lists(self.website_url)
        except Exception as e:
            self.logger.error(f"India Mopng scrapper failed at getting list of PDFs, error was {e}")
            raise Exception(e)

    def extract(self):
        self.get_list_of_pdfs()

        self.logger.info("Extracting PDF data")
        for pdf_path in self.list_of_pdfs:
            self.dataframes = get_pdf_info(pdf_path, self.dataframes, self.main_variables)
        # Converting the last child elements into Dataframe
        for index, key in enumerate(self.dataframes['crude_dataframes']):
            crude_temp_variable = self.dataframes['crude_dataframes'][key]
            if len(crude_temp_variable) > 0:
                self.dataframes['crude_dataframes'][key] = pd.DataFrame()
                self.dataframes['crude_dataframes'][key] = pd.concat(crude_temp_variable, ignore_index=True)

        # Converting the last child elements into Dataframe
        for index, key in enumerate(self.dataframes['petroleum_dataframes']):
            petroleum_temp_variable = self.dataframes['petroleum_dataframes'][key]
            if len(petroleum_temp_variable) > 0:
                self.dataframes['petroleum_dataframes'][key] = pd.DataFrame()
                self.dataframes['petroleum_dataframes'][key] = pd.concat(petroleum_temp_variable, ignore_index=True)
        self.logger.info("Extracted PDF data")

    def transform(self):
        self.logger.info("Transforming data")

        # Example Crude Data positions and new names
        crude_column_positions_to_rename = {0: 'Name of Undertaking/Unit/State',
                                            1: 'Production during the Preceding month of current year'}

        # Rename Crude Data columns based on positions
        for index, key in enumerate(self.dataframes['crude_dataframes']):
            for position, new_name in crude_column_positions_to_rename.items():
                # Ensure the position is within the range of existing columns
                if len(self.dataframes['crude_dataframes'][key]) > 0 and position < len(
                        self.dataframes['crude_dataframes'][key].columns):
                    self.dataframes['crude_dataframes'][key].rename(
                        columns={self.dataframes['crude_dataframes'][key].columns[position]: new_name}, inplace=True)

        # for index, key in enumerate(self.dataframes['crude_dataframes']):
        #     self.dataframes['crude_dataframes'][key].rename(crude_column_positions_to_rename)

        # Example Petroleum Data positions and new names
        petroleum_column_positions_to_rename = {0: 'Name of Undertaking/Unit/State',
                                                1: 'Production during the Preceding month of current year'}

        # Rename Petroleum Data columns based on positions
        for index, key in enumerate(self.dataframes['petroleum_dataframes']):
            for position, new_name in petroleum_column_positions_to_rename.items():
                # Ensure the position is within the range of existing columns
                if len(self.dataframes['petroleum_dataframes'][key]) > 0 and position < len(
                        self.dataframes['petroleum_dataframes'][key].columns):
                    self.dataframes['petroleum_dataframes'][key].rename(
                        columns={self.dataframes['petroleum_dataframes'][key].columns[position]: new_name},
                        inplace=True)

        # for index, key in enumerate(self.dataframes['petroleum_dataframes']):
        #     self.dataframes['petroleum_dataframes'][key].rename(petroleum_column_positions_to_rename)

        print(self.dataframes['crude_dataframes'])
        print(self.dataframes['petroleum_dataframes'])
        self.logger.info("Transformed Data")
