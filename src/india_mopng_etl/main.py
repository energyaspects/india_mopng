import pandas as pd

from helper_functions_ea import Logger
from india_mopng_etl.metadata import metadata
from india_mopng_etl.utils.base_classes import DataExtractor


class __Class_Name__(DataExtractor):  # make sure you rename the class to your preference
    """Make sure you implement all the methods required for your ETL"""

    logger = Logger("__Class_Name__").logger  # Creates a logger

    def __init__(self, ):
        """Setting the metadata (if needed) and any other needed dependencies."""
        self.metadata_df = metadata

    def extract(self):
        self.logger.info("Extracting data")
        self.df = pd.DataFrame()

    def transform(self, data):
        self.logger.info("Transforming data")
        self.df = self.df.merge(self.metadata_df)


def main():
    """
    Main function executes script
    Returns:
      None
    """
    class_init = __Class_Name__()
    class_init.etl()


if __name__ == "__main__":  # pragma: no cover
    main()
