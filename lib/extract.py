# -*- coding: utf-8 -*-
import json
import sys

import requests
import pandas as pd
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import StringIO
from json import JSONDecodeError
from .config import logger


class CsvDataExtractionError(Exception):
    """An error occurred during csv data extraction"""


class UnknownResponseDataExtractionError(Exception):
    """Unsuccessful response received from the API"""


@dataclass
class DataExtraction(ABC):
    """A Base class for data extraction strategy."""

    day: str = None
    url: str = None

    @abstractmethod
    def etl_data(self):
        raise NotImplementedError()

    @property
    def request_data(self) -> str:
        trial = 1
        max_trial = 10
        trial_sec = 5
        try:
            while trial <= max_trial:
                response = requests.get(self.url, timeout=5)

                if response.status_code == 200:
                    logger.info("The request %s succeeded.", self.url)
                    break
                elif response.status_code == 429:
                    logger.info(
                        "Too many requests have been sent. Trying again in %s seconds",
                        trial_sec,
                    )
                    trial += 1
                    time.sleep(trial_sec)

                else:
                    raise UnknownResponseDataExtractionError(
                        "The request didn't succeed."
                    )

        except requests.exceptions.Timeout as e:
            logger.error("Timed out: %s", str(e))
            sys.exit(1)
        except requests.RequestException as e:
            logger.error("Error: %s", str(e))
            sys.exit(1)

        return response.text


@dataclass
class JsonDataExtraction(DataExtraction):
    """Data Extraction class only for JSON sources"""

    @property
    def etl_data(self) -> pd.DataFrame:
        try:
            df = pd.DataFrame(json.loads(self.request_data))
            logger.info("JSON Data extracted from %s API", self.url)
        except JSONDecodeError as e:
            logger.error("JSON Data Extraction failed: %s", str(e))
            sys.exit(1)

        return df


@dataclass
class CsvDataExtraction(DataExtraction):
    """Data Extraction class only for CSV sources"""

    @property
    def etl_data(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(StringIO(self.request_data), sep=",")
            logger.info("CSV Data extracted from %s API", self.url)
        except Exception as exc:
            raise CsvDataExtractionError("CSV Data Extraction failed") from exc

        return df
