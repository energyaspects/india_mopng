"""ANY ADDITIONAL HELPER FUNCTIONS GOES HERE"""
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import tabula
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
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/mprsep2023.pdf']
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-april22.pdf']


def get_pdf_info(pdf_path, dataframes):
    # print(pdf_path)
    tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, lattice=True)

    target_header_for_crude = "Crude Oil Production during the month of"
    target_header_for_petroleum = "Production of Petroleum Products during the month of"
    crude_target_table = None
    petroleum_target_table = None

    for table in tables:
        if table.shape[0] > 0 and pd.notnull(table.iloc[0, 0]) and target_header_for_crude in str(table.iloc[0, 0]):
            crude_target_table = table
        elif table.shape[0] > 0 and pd.notnull(table.iloc[0, 0]) and target_header_for_petroleum in str(
                table.iloc[0, 0]):
            petroleum_target_table = table
        else:
            pass
        if crude_target_table is not None and petroleum_target_table is not None:
            break

    if crude_target_table is not None and petroleum_target_table is not None:
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
                # dataframes['crude_dataframes'].append(crude_target_table)
                if len(crude_target_table) >= len(dataframes['crude_dataframes']):
                    for index, key in enumerate(dataframes['crude_dataframes']):
                        dataframes['crude_dataframes'][key].append(crude_target_table.iloc[[index]])
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
                # dataframes['petroleum_dataframes'].append(petroleum_target_table)
                if len(petroleum_target_table) >= len(dataframes['petroleum_dataframes']):
                    for index, key in enumerate(dataframes['petroleum_dataframes']):
                        dataframes['petroleum_dataframes'][key].append(petroleum_target_table.iloc[[index]])
    else:
        print("Target header not found in any table.")
    return dataframes
