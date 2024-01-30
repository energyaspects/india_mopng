from india_mopng_etl.utils import *


class BaseVariables:
    arguments = parsed_args
    environment = arguments.environment
    prefix = arguments.prefix


class DataExtractor:
    base_variables = BaseVariables()
    shooju_handler = SHOOJU_HANDLER
    sql_handler = SQL_HANDLER
    selenium_handler = SELENIUM_HANDLER

    remove_others = None
    repdate = None
    df = None

    def extract(self, **kwargs):
        """Extracts the data from the source"""
        raise NotImplementedError

    def transform(self, **kwargs):
        """Transforms the data"""
        raise NotImplementedError

    def load(self):
        """Uses the pre-registered job to load the data from the ETL in shooju using upload long."""
        self.shooju_handler.df_upload_long(
            df=self.df, job_name="Uploading products total delivery costs", sid_prefix=self.base_variables.prefix,
            job=registered_job, repdate=self.repdate, remove_others=self.remove_others
        )

    def etl(self):
        try:
            self.extract()
        except Exception as err:
            raise RuntimeError(f"Scraper failed at extraction. Error was {err}") from err
        try:
            self.transform()
        except Exception as err:
            raise RuntimeError(f"Scraper failed at dataframe transformation. Error was {err}") from err
        try:
            self.load()
        except Exception as err:
            raise RuntimeError(f"Scraper failed at upload. Error was {err}") from err
