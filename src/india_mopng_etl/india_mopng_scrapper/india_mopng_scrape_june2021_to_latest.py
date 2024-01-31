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
    WEBSITE_URL = "https://mopng.gov.in/en/petroleum-statistics/monthly-production"

    def __init__(self):
        self.list_of_pdfs = []
        self.crude_df = pd.DataFrame()
        self.petroleum_df = pd.DataFrame()
        # self.dataframes = {'crude_dataframes': [], 'petroleum_dataframes': []}
        self.dataframes = {
            'crude_dataframes': {
                "MOPNG:India ONGC total monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC total onshore monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC onshore Andhra Pradesh monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC onshore Assam monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC onshore Gujarat monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC onshore Tamil Nadu monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC onshore Tripura monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC total offshore monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC eastern offshore monthly production of crude oil in Kt": [],
                "MOPNG:India ONGC western offshore monthly production of crude oil in Kt": [],
                "MOPNG:India OIL total onshore monthly production of crude oil in Kt": [],
                "MOPNG:India OIL onshore Assam monthly production of crude oil in Kt": [],
                "MOPNG:India OIL onshore Arunachal Pradesh monthly production of crude oil in Kt": [],
                "MOPNG:India OIL onshore Rajasthan monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC total monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC total onshore monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC onshore Andhra Pradesh monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC onshore Arunachal Pradesh monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC onshore Assam monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC onshore Gujarat monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC onshore Rajasthan monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC onshore Tamil Nadu monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC onshore West Bengal monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC total offshore monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC eastern offshore monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC gujarat offshore monthly production of crude oil in Kt": [],
                "MOPNG:India Private/JVC western offshore monthly production of crude oil in Kt": [],
                "MOPNG:India total monthly production of crude oil in Kt": [],
                "MOPNG:India total onshore monthly production of crude oil in Kt": [],
                "MOPNG:India total offshore monthly production of crude oil in Kt": [],
            },
            'petroleum_dataframes': {
                "MOPNG:India CPSE's refineries total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Guwahati total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Barauni total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Gujarat total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Haldia total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Mathura total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Digboi total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Panipat total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Bongaigaon total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC Paradip total monthly production of NGL's in Kt": [],
                "MOPNG:India IOC total monthly production of NGL's in Kt": [],
                "MOPNG:India BPCL Mumbai monthly production of NGL's in Kt": [],
                "MOPNG:India BPCL Kochi monthly production of NGL's in Kt": [],
                "MOPNG:India BPCL Bina monthly production of NGL's in Kt": [],
                "MOPNG:India BPCL total monthly production of NGL's in Kt": [],
                "MOPNG:India HPCL Mumbai monthly production of NGL's in Kt": [],
                "MOPNG:India HPCL Visakh monthly production of NGL's in Kt": [],
                "MOPNG:India HPCL total monthly production of NGL's in Kt": [],
                "MOPNG:India CPCL Manali monthly production of NGL's in Kt": [],
                "MOPNG:India NRL Numaligarh monthly production of NGL's in Kt": [],
                "MOPNG:India MRPL Mangalore monthly production of NGL's in Kt": [],
                "MOPNG:India ONGC Ttipaka monthly production of NGL's in Kt": [],
                "MOPNG:India Private and JV's refineries total monthly production of NGL's in Kt": [],
                "MOPNG:India HMEL,GGSR,Bhatinda monthly production of NGL's in Kt": [],
                "MOPNG:India RIL Jamnagar monthly production of NGL's in Kt": [],
                "MOPNG:India RIL SEZ monthly production of NGL's in Kt": [],
                "MOPNG:India Total RIL": [],
                "MOPNG:India NEL Vadinar monthly production of NGL's in Kt": [],
                "MOPNG:India refineries total monthly production of NGL's in Kt": [],
                "MOPNG:India ONGC monthly production of NGL's in Kt": [],
                "MOPNG:India GAIL monthly production of NGL's in Kt": [],
                "MOPNG:India OIL monthly production of NGL's in Kt": [],
                "MOPNG:India monthly production of NGL's in Kt": [],
                "MOPNG:India total monthly production of NGL's in Kt": [],
            }
        }

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

        self.logger.info("Extracting PDF data")
        for pdf_path in self.list_of_pdfs:
            self.dataframes = get_pdf_info(pdf_path, self.dataframes)
        # self.crude_df = pd.concat(self.dataframes['crude_dataframes'], ignore_index=True)
        # self.petroleum_df = pd.concat(self.dataframes['petroleum_dataframes'], ignore_index=True)
        for index, key in enumerate(self.dataframes['crude_dataframes']):
            crude_temp_variable = self.dataframes['crude_dataframes'][key]
            self.dataframes['crude_dataframes'][key] = pd.DataFrame()
            self.dataframes['crude_dataframes'][key] = pd.concat(crude_temp_variable, ignore_index=True)

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

        # print(self.crude_df)
        # print(self.petroleum_df)

        print(self.dataframes['crude_dataframes'])
        self.logger.info("Transformed Data")
