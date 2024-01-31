from india_mopng_etl.india_mopng_scrapper.india_mopng_scrape_june2023_to_latest import June2023ToLatest


def main():
    """
    Main function executes script
    Returns:
      None
    """
    try:
        class_init = June2023ToLatest()
        class_init.etl()
    except Exception as ex:
        raise RuntimeError(f"Scraper failed to process. Error was {ex}")


if __name__ == "__main__":  # pragma: no cover
    main()
