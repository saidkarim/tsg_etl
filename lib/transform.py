# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
import pandas as pd


class Transform(ABC):
    """A Base class for staging data transformation."""

    @abstractmethod
    def perform(self):
        raise NotImplementedError()

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        return df


class SolarTransform(Transform):
    """Staging data transformation for Solar ETL"""

    @staticmethod
    def _fix_column_types(df: pd.DataFrame) -> pd.DataFrame:
        df["naive_timestamp"] = pd.to_datetime(df["naive_timestamp"], unit="ms")
        df["last_modified_utc"] = pd.to_datetime(df["last_modified_utc"], unit="ms")
        df["variable"] = df["variable"].astype(int)
        df["value"] = df["value"].astype(float)
        df["aware_timestamp"] = pd.to_datetime(
            df["naive_timestamp"], unit="ms", utc=True
        )
        return df

    def perform(self, data: pd.DataFrame) -> pd.DataFrame:
        data = self._normalize_columns(data)
        data = self._fix_column_types(data)

        # Drop column Naive timestamp
        return data.drop("naive_timestamp", axis=1)


class WindTransform(Transform):
    """Staging data transformation for Wind ETL"""

    @staticmethod
    def _fix_column_types(df: pd.DataFrame) -> pd.DataFrame:
        df["naive_timestamp"] = pd.to_datetime(df["naive_timestamp"])
        df["last_modified_utc"] = pd.to_datetime(df["last_modified_utc"])
        df["variable"] = df["variable"].astype(int)
        df["value"] = df["value"].astype(float)
        df["aware_timestamp"] = pd.to_datetime(
            df["naive_timestamp"], unit="ms", utc=True
        )
        return df

    def perform(self, data: pd.DataFrame) -> pd.DataFrame:
        data = self._normalize_columns(data)
        data = self._fix_column_types(data)

        # Drop column Naive timestamp
        return data.drop("naive_timestamp", axis=1)
