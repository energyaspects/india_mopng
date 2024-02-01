from india_mopng_etl.india_mopng_scrapper.india_mopng_scrape_june2021_to_latest import Scrapper


def main():
    """
    Main function executes script
    Returns:
      None
    """
    # website_url = "https://mopng.gov.in/en/petroleum-statistics/monthly-production"
    website_url = "https://mopng.gov.in/en/petroleum-statistics/monthly-production?year_range=2022"
    dataframes = {
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
    try:
        class_init = Scrapper(website_url, dataframes)
        class_init.etl()
    except Exception as ex:
        raise RuntimeError(f"Scrapper failed to process. Error was {ex}")


if __name__ == "__main__":  # pragma: no cover
    main()
