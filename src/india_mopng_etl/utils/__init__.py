import os
import argparse
from datetime import datetime
from helper_functions_ea import ShoojuTools, check_env

check_env()

# Initiate all your handlers here

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
                        default="dev",
                        choices=["tests", "prod", "dev"])

    args, _ = parser.parse_known_args()

    if args.environment == "prod":
        args.prefix = "teams\\products"
        args.SQL_DEBUG_MODE = False
    elif args.environment == "tests":
        args.prefix = "tests\\products"
        args.SQL_DEBUG_MODE = True
    else:
        args.prefix = f"users\\"+os.environ["SHOOJU_USER"]+"\\products"
        args.SQL_DEBUG_MODE = True
    return args


parsed_args = parse_args()

registered_job = SHOOJU_ENGINE.register_job(
    description="Registered Job to write data in shooju",
    batch_size=5000
)

__all__ = ["SHOOJU_HANDLER", "SHOOJU_ENGINE", "parsed_args", "registered_job"]
