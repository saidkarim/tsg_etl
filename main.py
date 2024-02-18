from concurrent.futures import ThreadPoolExecutor
from lib.utils import last_weeks_day
from lib.etl import ETL
from lib.extract import JsonDataExtraction, CsvDataExtraction
from lib.transform import SolarTransform, WindTransform
from lib.config import logger


def run_solar_etl_daily(day: str) -> None:
    logger.info("Solar ETL for day %s started...", day)

    # Set up extraction
    solar_url = f"http://127.0.0.1:8000/{day}/renewables/solargen.json?api_key=ADU8S67Ddy%21d7f%3F"
    solar_data_extraction = JsonDataExtraction(url=solar_url, day=day)

    # Set up transform
    solar_transform = SolarTransform()

    # Run ETL
    solar_etl = ETL(
        name="solar", data_extraction=solar_data_extraction, transform=solar_transform
    )
    solar_etl.run()

    logger.info("Solar ETL for day %s successfully finished...", day)


def run_solar_etl_weekly() -> None:
    with ThreadPoolExecutor() as solar_executor:
        solar_executor.map(run_solar_etl_daily, last_weeks_day())


def run_wind_etl_daily(day: str) -> None:
    logger.info("Wind ETL for day %s started...", day)

    # Set up extraction
    wind_url = f"http://127.0.0.1:8000/{day}/renewables/windgen.csv?api_key=ADU8S67Ddy%21d7f%3F"
    wind_data_extraction = CsvDataExtraction(url=wind_url, day=day)

    # Set up transform
    wind_transform = WindTransform()

    # Run ETL
    wind_etl = ETL(
        name="wind", data_extraction=wind_data_extraction, transform=wind_transform
    )
    wind_etl.run()

    logger.info("Wind ETL for day %s successfully finished...", day)


def run_wind_etl_weekly() -> None:
    with ThreadPoolExecutor() as wind_executor:
        wind_executor.map(run_wind_etl_daily, last_weeks_day())


if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        executor.submit(run_solar_etl_weekly)
        executor.submit(run_wind_etl_weekly)
