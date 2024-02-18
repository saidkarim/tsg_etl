import pytest
from unittest.mock import Mock, patch, MagicMock
from lib.etl import ETL, OUTPUT_DIR


@pytest.fixture
def mock_data_extraction():
    mock = Mock()
    mock.etl_data = {"data": "extracted data"}
    mock.day = "2022-11-15"
    return mock


@pytest.fixture
def mock_transform():
    mock = Mock()
    mock.perform.return_value = {"data": "transformed data"}
    return mock


@pytest.fixture
def etl_instance(mock_data_extraction, mock_transform):
    return ETL(
        name="test_etl", data_extraction=mock_data_extraction, transform=mock_transform
    )


def test_extract(etl_instance, mock_data_extraction):
    etl_instance.extract()
    assert (
        etl_instance.etl_data == mock_data_extraction.etl_data
    ), "Extract method does not correctly assign data"


def test_perform_transformation(etl_instance, mock_transform):
    etl_instance.extract()
    etl_instance.perform_transformation()
    assert (
        etl_instance.etl_data == mock_transform.perform.return_value
    ), "Transformation does not correctly modify data"


@patch("lib.etl.os.path.join", return_value="/fake/dir/test_etl_2022-11-15.json")
def test_load(mock_path_join, etl_instance, mock_data_extraction):
    etl_instance.etl_data = MagicMock()
    etl_instance.load()
    mock_path_join.assert_called_once_with(OUTPUT_DIR, "test_etl_2022-11-15.json")
