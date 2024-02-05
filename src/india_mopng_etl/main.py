# from india_mopng_etl.india_mopng_scrapper.india_mopng_scrape_2023 import Scrapper
# from india_mopng_etl.india_mopng_scrapper.india_mopng_scrape_2022 import Scrapper
from india_mopng_etl.india_mopng_scrapper.india_mopng_scrape_2021 import Scrapper


def main():
    """
    Main function executes script
    Returns:
      None
    """
    # website_url = "https://mopng.gov.in/en/petroleum-statistics/monthly-production"
    # website_url = "https://mopng.gov.in/en/petroleum-statistics/monthly-production?year_range=2023"
    website_url = "https://mopng.gov.in/en/petroleum-statistics/monthly-production?year_range=2021"
    main_variables = {
        'crude_main_variables': ['1', '2', '3', 'grand']
    }
    dataframes = {
        'crude_dataframes': {
            "ongc": [],
            "ongc_onshore": [],
            "ongc_onshore_andhra_pradesh": [],
            "ongc_onshore_assam": [],
            "ongc_onshore_gujarat": [],
            "ongc_onshore_tamil_nadu": [],
            "ongc_onshore_tripura": [],
            "ongc_offshore": [],
            "ongc_offshore_eastern_offshore": [],
            "ongc_offshore_western_offshore": [],
            "ongc_offshore_condensates": [],
            "oil_onshore": [],
            "oil_onshore_assam": [],
            "oil_onshore_arunachal_pradesh": [],
            "oil_onshore_rajasthan": [],
            "private": [],
            "private_onshore": [],
            "private_onshore_andhra_pradesh": [],
            "private_onshore_arunachal_pradesh": [],
            "private_onshore_assam": [],
            "private_onshore_gujarat": [],
            "private_onshore_rajasthan": [],
            "private_onshore_tamil_nadu": [],
            "private_onshore_west_bengal": [],
            "private_offshore": [],
            "private_offshore_eastern_offshore": [],
            "private_offshore_gujarat_offshore": [],
            "private_offshore_western_offshore": [],
            "grand_total": [],
            "grand_total_onshore": [],
            "grand_total_offshore": [],
        },
        'petroleum_dataframes': {
            "cpse": [],
            "ioc_guwahati": [],
            "ioc_barauni": [],
            "ioc_gujarat": [],
            "ioc_haldia": [],
            "ioc_mathura": [],
            "ioc_digboi": [],
            "ioc_panipat": [],
            "ioc_bongaigaon": [],
            "ioc_paradip": [],
            "total_ioc": [],
            "bpcl_mumbai": [],
            "bpcl_kochi": [],
            "bpcl_bina": [],
            "total_bpcl": [],
            "hpcl_mumbai": [],
            "hpcl_visakh": [],
            "total_hpcl": [],
            "cpcl_manali": [],
            "nrl_numaligarh": [],
            "mrpl_mangalore": [],
            "ongc_tatipaka": [],
            "private": [],
            "hmel_ggsr_bhatinda": [],
            "ril_jamnagar": [],
            "ril_sez": [],
            "total_ril": [],
            "nel_vadinar": [],
            "total_refinery": [],
            "ongc": [],
            "gail": [],
            "oil": [],
            "total_fractionator": [],
            "grand_total": [],
        }
    }
    try:
        class_init = Scrapper(website_url, dataframes, main_variables)
        class_init.etl()
    except Exception as ex:
        raise RuntimeError(f"Scrapper failed to process. Error was {ex}")


if __name__ == "__main__":  # pragma: no cover
    main()
