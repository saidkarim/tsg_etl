import pytest
import pandas as pd
from lib.transform import SolarTransform, WindTransform


@pytest.fixture
def wind_sample_data():
    data = {
        "Naive TimeStamp": ["2024-01-24 01:15:00+00:00", "2024-01-24 01:15:00+00:00"],
        "Last Modified UTC": ["2024-01-24 01:15:00+00:00", "2024-01-24 01:15:00+00:00"],
        "VariAble": ["1", "2"],
        "ValUe": ["100.5", "200.5"],
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def solar_sample_data():
    data = {
        "Naive Timestamp ": [1609459200000, 1609545600000],
        "Last Modified UTC": [1609459200000, 1609545600000],
        "Variable ": ["1", "2"],
        " Value": ["100.5", "200.5"],
    }
    df = pd.DataFrame(data)
    return df


@pytest.mark.parametrize("transform_class", [SolarTransform, WindTransform])
def test_normalize_columns(solar_sample_data, transform_class):
    transformer = transform_class()
    normalized_df = transformer._normalize_columns(solar_sample_data)
    expected_columns = ["naive_timestamp", "last_modified_utc", "variable", "value"]
    assert (
        list(normalized_df.columns) == expected_columns
    ), "Column names are not correctly normalized"


def test_fix_column_types_solar(solar_sample_data):
    transformer = SolarTransform()
    normalized_df = transformer._normalize_columns(solar_sample_data)
    fixed_df = transformer._fix_column_types(normalized_df)
    assert pd.api.types.is_datetime64_any_dtype(
        fixed_df["naive_timestamp"]
    ), "Naive timestamp is not a datetime type"
    assert pd.api.types.is_datetime64_any_dtype(
        fixed_df["last_modified_utc"]
    ), "Last Modified UTC is not a datetime type"
    assert pd.api.types.is_integer_dtype(
        fixed_df["variable"]
    ), "Variable is not an integer type"
    assert pd.api.types.is_float_dtype(fixed_df["value"]), "Value is not a float type"


def test_fix_column_types_wind(wind_sample_data):
    transformer = WindTransform()
    normalized_df = transformer._normalize_columns(wind_sample_data)
    fixed_df = transformer._fix_column_types(normalized_df)
    assert pd.api.types.is_datetime64_any_dtype(
        fixed_df["naive_timestamp"]
    ), "Naive timestamp is not a datetime type"
    assert pd.api.types.is_datetime64_any_dtype(
        fixed_df["last_modified_utc"]
    ), "Last Modified UTC is not a datetime type"
    assert pd.api.types.is_integer_dtype(
        fixed_df["variable"]
    ), "Variable is not an integer type"
    assert pd.api.types.is_float_dtype(fixed_df["value"]), "Value is not a float type"


@pytest.mark.parametrize("transform_class", [SolarTransform, WindTransform])
def test_perform_drops_naive_timestamp(solar_sample_data, transform_class):
    transformer = transform_class()
    transformed_df = transformer.perform(solar_sample_data)
    assert (
        "naive_timestamp" not in transformed_df.columns
    ), "Naive timestamp column was not dropped"
