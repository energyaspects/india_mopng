import requests
from bs4 import BeautifulSoup
# import io
# from PyPDF2 import PdfReader
from urllib.parse import urljoin
import tabula
import pandas as pd


def get_list_of_pdfs(url):
    # # website to scrap
    # # url = "https://www.geeksforgeeks.org/how-to-extract-pdf-tables-in-python/"
    # url = url
    # read = requests.get(url)
    # html_content = read.content
    # soup = BeautifulSoup(html_content, "html.parser")
    #
    # # created an empty list for putting the pdfs
    # list_of_pdf = []
    #
    # # accessed all the anchors tags
    # anchor_tags = soup.find_all('a')
    #
    # # iterate through anchor tags for checking the extension is .pdf
    # for link in anchor_tags:
    #     if link.get('href').endswith('.pdf'):
    #         if not link.get('href').startswith('http'):
    #             list_of_pdf.append(urljoin(url, link.get('href')))
    #         else:
    #             # Adding the pdf links into the list_of_pdf
    #             list_of_pdf.append(link.get('href'))
    # return list_of_pdf
    return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPR-for-the-month-of-Feb,2021.pdf']
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/mprsep2023.pdf']
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/MPRjan2023.pdf']
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/mprjuly17.pdf']
    # return ['https://mopng.gov.in/files/petroleumStatistics/monthlyProduction/ilovepdf_merged.pdf']


def get_pdf_info(pdf_path):
    print(pdf_path)
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

                # Rename columns based on positions
                # Example positions and new names
                column_positions_to_rename = {0: 'Name of Undertaking/Unit/State',
                                              4: 'Production during the Preceding month of current year'}

                # Rename columns based on positions
                for position, new_name in column_positions_to_rename.items():
                    if position < len(
                            crude_target_table.columns):  # Ensure the position is within the range of existing columns
                        crude_target_table.rename(columns={crude_target_table.columns[position]: new_name},
                                                  inplace=True)

            # Select only the first and fifth columns
            crude_target_table = crude_target_table.iloc[:,
                                 [0, 4]]  # Select all rows, and the first and fifth columns (zero-indexed)
            print("*****************************CRUDE*************************")
            print(crude_target_table)
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

                # Rename columns based on positions
                # Example positions and new names
                column_positions_to_rename = {0: 'Name of Undertaking/Unit/State',
                                              4: 'Production during the Preceding month of current year'}

                # Rename columns based on positions
                for position, new_name in column_positions_to_rename.items():
                    if position < len(
                            petroleum_target_table.columns):  # Ensure the position is within the range of existing columns
                        petroleum_target_table.rename(columns={petroleum_target_table.columns[position]: new_name},
                                                      inplace=True)

            # Select only the first and fifth columns
            petroleum_target_table = petroleum_target_table.iloc[:,
                                     [0, 4]]  # Select all rows, and the first and fifth columns (zero-indexed)
            print("*****************************PETROLEUM*************************")
            print(petroleum_target_table)
    else:
        print("Target header not found in any table.")


def main():
    url = "https://mopng.gov.in/en/petroleum-statistics/monthly-production"
    list_of_pdf = get_list_of_pdfs(url)
    for i in list_of_pdf:
        get_pdf_info(i)


if __name__ == "__main__":  # pragma: no cover
    main()
