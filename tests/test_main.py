import argparse
import pytest
import pandas as pd
from india_mopng_etl.main import __Class_Name__
from india_mopng_etl import main

dummy_data = pd.DataFrame()


def test_extract():
    assert isinstance(__Class_Name__().extract(), pd.DataFrame)


def test_transform():
    assert isinstance(__Class_Name__().transform(dummy_data), pd.DataFrame)


def test_main():
    try:
        main.main()
    except Exception as e:
        raise e
