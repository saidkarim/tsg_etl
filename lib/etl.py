# -*- coding: utf-8 -*-
import os
from .config import OUTPUT_DIR, logger
from .extract import DataExtraction
from .transform import Transform


class ETL:
    """An interface class for ETL process"""

    def __init__(
        self, name: str, data_extraction: DataExtraction, transform: Transform
    ):
        self.data_extraction = data_extraction
        self.etl_data = None
        self.transform = transform
        self.name = name

    def run(self) -> None:
        self.extract()
        self.perform_transformation()
        self.load()

    def extract(self) -> None:
        self.etl_data = self.data_extraction.etl_data

    def perform_transformation(self) -> None:
        self.etl_data = self.transform.perform(self.etl_data)

    def load(self) -> None:
        filename = os.path.join(
            OUTPUT_DIR, f"{self.name}_{self.data_extraction.day}.json"
        )
        self.etl_data.to_json(filename, orient="records", indent=4)
