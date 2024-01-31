import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import tabula
import pandas as pd

from helper_functions_ea import Logger
from india_mopng_etl.utils.base_classes import DataExtractor
from india_mopng_etl.utils.helper_functions import get_pdf_lists, get_pdf_info


class June2023ToLatest(DataExtractor):  # make sure you rename the class to your preference
    """Make sure you implement all the methods required for your ETL"""

    logger = Logger("India Mopng Scrape => ").logger
    # name = "India Mopng Scrape ETL"
    WEBSITE_URL = "https://mopng.gov.in/en/petroleum-statistics/monthly-production"

    def __init__(self):
        self.list_of_pdfs = []
        self.crude_df = pd.DataFrame()
        self.petroleum_df = pd.DataFrame()

    def get_list_of_pdfs(self):
        """
        Gets the PDF links from the website
        """
        self.logger.info("Getting List of PDFs")
        try:
            self.list_of_pdfs = get_pdf_lists(self.WEBSITE_URL)
        except Exception as e:
            self.logger.error(f"India Mopng scrapper failed at getting list of PDFs, error was {e}")
            raise Exception(e)

    def extract(self):
        self.get_list_of_pdfs()

        dataframes = {'crude_dataframe': [], 'petroleum_dataframe': []}
        self.logger.info("Extracting PDF data")
        for pdf_path in self.list_of_pdfs:
            dataframes = get_pdf_info(pdf_path, dataframes)
        self.crude_df = pd.concat(dataframes['crude_dataframe'], ignore_index=True)
        self.petroleum_df = pd.concat(dataframes['petroleum_dataframe'], ignore_index=True)
        self.logger.info("Extracted PDF data")

    def transform(self):
        self.logger.info("Transforming data")
        # Example Crude Data positions and new names
        crude_column_positions_to_rename = {0: 'Name of Undertaking/Unit/State',
                                            1: 'Production during the Preceding month of current year'}

        # Rename Crude Data columns based on positions
        for position, new_name in crude_column_positions_to_rename.items():
            # Ensure the position is within the range of existing columns
            if position < len(self.crude_df.columns):
                self.crude_df.rename(columns={self.crude_df.columns[position]: new_name}, inplace=True)

        # Example Petroleum Data positions and new names
        petroleum_column_positions_to_rename = {0: 'Name of Undertaking/Unit/State',
                                                1: 'Production during the Preceding month of current year'}

        # Rename Petroleum Data columns based on positions
        for position, new_name in petroleum_column_positions_to_rename.items():
            # Ensure the position is within the range of existing columns
            if position < len(self.petroleum_df.columns):
                self.petroleum_df.rename(columns={self.petroleum_df.columns[position]: new_name}, inplace=True)
        print(self.crude_df)
        print(self.petroleum_df)
        self.logger.info("Transformed Data")
