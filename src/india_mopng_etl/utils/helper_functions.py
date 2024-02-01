"""ANY ADDITIONAL HELPER FUNCTIONS GOES HERE"""
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import tabula
import datetime
import pandas as pd


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
    # return [
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPRjanuary2022.pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-for-Feb22.pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/march2022.pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-april22.pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MRP-for-May-2022.pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-for-the-month-of-June,2022.pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/ilovepdf_merged-(6).pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-For-the-month-of-Aug,2022-(2).pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-for-Sep-2022.pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-Nov,22_merged.pdf",
    #     "https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPRDEC2022.pdf"
    # ]
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/mprsep2023.pdf']
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-april22.pdf']


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


def get_pdf_info(pdf_path, dataframes):
    # print(pdf_path)
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
        # Checking the Petroleum header is present or not
        elif table.shape[0] > 0 and pd.notnull(table.iloc[0, 0]) and target_header_for_petroleum in str(
                table.iloc[0, 0]):
            petroleum_target_table = table
            petroleum_target_table_header = str(table.iloc[0, 0])
        else:
            pass
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
                if len(crude_target_table) >= len(dataframes['crude_dataframes']):
                    for index, key in enumerate(dataframes['crude_dataframes']):
                        crude_table_with_required_columns = crude_target_table.iloc[[index]]
                        crude_table_with_required_columns['datetime'] = pd.to_datetime(
                            crude_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['crude_dataframes'][key].append(crude_table_with_required_columns)
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
                if len(petroleum_target_table) >= len(dataframes['petroleum_dataframes']):
                    for index, key in enumerate(dataframes['petroleum_dataframes']):
                        petroleum_table_with_required_columns = petroleum_target_table.iloc[[index]]
                        petroleum_table_with_required_columns['datetime'] = pd.to_datetime(
                            petroleum_month_year_datetime_object.strftime('%Y-%m-%d %H:%M:%S'))
                        dataframes['petroleum_dataframes'][key].append(petroleum_table_with_required_columns)
    else:
        print("Target header not found in any table.")
    return dataframes
