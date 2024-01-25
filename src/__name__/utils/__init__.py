import argparse
from datetime import datetime
from helper_functions_ea import Selenium, ShoojuTools, SqlEngine, check_env

check_env()

# Initiate all your handlers here

SELENIUM_HANDLER = Selenium(headless=False)
SHOOJU_HANDLER = ShoojuTools()

# Set any engines here
SHOOJU_ENGINE = SHOOJU_HANDLER.sj


def parse_args() -> argparse.Namespace:
    """
    Argument parser

    Returns:
        Args: an argparse object
    """

    parser = argparse.ArgumentParser(description="Parameters for ETL")

    parser.add_argument("--environment",
                        help="Environment you would like to run the project in",
                        default="tests",
                        choices=["tests", "prod", "dev"])

    args, _ = parser.parse_known_args()

    if args.environment == "prod":
        args.sj_prefix = "teams\\products"
        args.SQL_DEBUG_MODE = False
    elif args.environment == "tests":
        args.sj_prefix = "tests\\products"
        args.SQL_DEBUG_MODE = True
    else:
        args.sj_prefix = f"users\\{SHOOJU_ENGINE.user}\\products"
        args.SQL_DEBUG_MODE = True
    return args


parsed_args = parse_args()

SQL_HANDLER = SqlEngine(
    DEBUG=parsed_args.SQL_DEBUG_MODE,
    LOG=False,
    db_url_raw="postgresql+psycopg2://%(user)s:%(passwd)s@%(host)s:%(port)s/%(db)s",
)

SQL_ENGINE = SQL_HANDLER.engine

registered_job = SHOOJU_ENGINE.register_job(
    description="Registered Job to write data in shooju",
    batch_size=5000
)

__all__ = ["SELENIUM_HANDLER", "SHOOJU_HANDLER", "SQL_HANDLER", "SHOOJU_ENGINE", "parsed_args", "registered_job"]
