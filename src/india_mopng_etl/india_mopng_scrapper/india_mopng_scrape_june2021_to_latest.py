import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import tabula
import pandas as pd

from helper_functions_ea import Logger
from india_mopng_etl.utils.base_classes import DataExtractor
from india_mopng_etl.utils.helper_functions import get_pdf_lists, get_pdf_info


class Scrapper(DataExtractor):  # make sure you rename the class to your preference
    """Make sure you implement all the methods required for your ETL"""

    logger = Logger("India Mopng Scrape => ").logger
    # name = "India Mopng Scrape ETL"

    def __init__(self, website_url, dataframes):
        self.list_of_pdfs = []
        self.crude_df = pd.DataFrame()
        self.petroleum_df = pd.DataFrame()
        self.website_url = website_url
        self.dataframes = dataframes

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
            self.dataframes = get_pdf_info(pdf_path, self.dataframes)

        # Converting the last child elements into Dataframe
        for index, key in enumerate(self.dataframes['crude_dataframes']):
            crude_temp_variable = self.dataframes['crude_dataframes'][key]
            self.dataframes['crude_dataframes'][key] = pd.DataFrame()
            self.dataframes['crude_dataframes'][key] = pd.concat(crude_temp_variable, ignore_index=True)

        # Converting the last child elements into Dataframe
        for index, key in enumerate(self.dataframes['petroleum_dataframes']):
            petroleum_temp_variable = self.dataframes['petroleum_dataframes'][key]
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
                if position < len(self.dataframes['crude_dataframes'][key].columns):
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
                if position < len(self.dataframes['petroleum_dataframes'][key].columns):
                    self.dataframes['petroleum_dataframes'][key].rename(
                        columns={self.dataframes['petroleum_dataframes'][key].columns[position]: new_name}, inplace=True)

        # for index, key in enumerate(self.dataframes['petroleum_dataframes']):
        #     self.dataframes['petroleum_dataframes'][key].rename(petroleum_column_positions_to_rename)

        print(self.dataframes['crude_dataframes'])
        print(self.dataframes['petroleum_dataframes'])
        self.logger.info("Transformed Data")
