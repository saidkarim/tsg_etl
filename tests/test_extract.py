import pytest
from unittest.mock import patch, MagicMock
from lib.extract import JsonDataExtraction, CsvDataExtraction, CsvDataExtractionError
import pandas as pd


@pytest.fixture
def mock_response_success():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    return mock_resp


@pytest.fixture
def mock_response_retry():
    mock_resp = MagicMock()
    mock_resp.status_code = 429
    return mock_resp


def test_json_data_extraction_success(mock_response_success):
    mock_response_success.text = '{"name": ["Said", "Gunel"], "age": [30, 25]}'
    with patch(
        "lib.extract.requests.get", return_value=mock_response_success
    ) as mock_get:
        extractor = JsonDataExtraction(url="http://test.com")
        df = extractor.etl_data
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2  # Checks if DataFrame has two rows
        mock_get.assert_called_once()


def test_csv_data_extraction_success(mock_response_success):
    mock_response_success.text = "name,age\nSaid,30\nGunel,25"
    with patch("lib.extract.requests.get", return_value=mock_response_success):
        extractor = CsvDataExtraction(url="http://test.com")
        df = extractor.etl_data
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2  # Checks if DataFrame has two rows


def test_data_extraction_retry(mock_response_retry, mock_response_success):
    mock_response_success.text = '{"name": ["Said", "Gunel"], "age": [30, 25]}'
    with patch(
        "lib.extract.requests.get",
        side_effect=[mock_response_retry, mock_response_success],
    ) as mock_get:
        extractor = JsonDataExtraction(url="http://test.com")
        df = extractor.etl_data
        assert mock_get.call_count == 2
        assert len(df) == 2


def test_csv_data_extraction_error():
    with patch(
        "lib.extract.requests.get", side_effect=Exception("CSV Data Extraction failed")
    ):
        extractor = CsvDataExtraction(url="http://test.com")
        with pytest.raises(CsvDataExtractionError):
            _ = extractor.etl_data
