from pathlib import Path
import pandas as pd

metadata = pd.read_excel(Path(__file__).parent / "metadata_example.xlsx",
                         sheet_name='Details for the Data scraped',  # make sure it points to the proper sheet name
                         index_col=0,  # product
                         header=5,
                         usecols='B:X').dropna(axis=1, how="all").dropna(axis=0, how="all")
