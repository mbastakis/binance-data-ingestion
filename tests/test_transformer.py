# tests/test_transformer.py

import pytest
import pandas as pd
from transformation.transformer import DataTransformer
from unittest.mock import MagicMock

@pytest.fixture
def sample_transformer():
    """
    Fixture to create a DataTransformer instance with a sample configuration.
    """
    config = {
        'resample_frequency': '1T',  # 1 Minute
        'symbols': ['AAPL']
    }
    transformer = DataTransformer(config)
    
    # Mock repositories to isolate transformer logic
    transformer.raw_data_repo = MagicMock()
    transformer.downsampled_repo = MagicMock()
    
    return transformer

@pytest.fixture
def valid_sample_df():
    """
    Fixture for a DataFrame with valid numeric prices.
    """
    return pd.DataFrame({
        'timestamp': pd.to_datetime([
            '2024-10-12 09:00:00',
            '2024-10-12 09:01:00',
            '2024-10-12 09:02:00',
            '2024-10-12 09:03:00'
        ]),
        'price': ['100', '101', '102', '103'],
        'symbol': ['AAPL', 'AAPL', 'AAPL', 'AAPL']
    })

@pytest.fixture
def invalid_price_df():
    """
    Fixture for a DataFrame with non-numeric prices.
    """
    return pd.DataFrame({
        'timestamp': pd.to_datetime([
            '2024-10-12 09:00:00',
            '2024-10-12 09:01:00',
            '2024-10-12 09:02:00'
        ]),
        'price': ['100', 'ABC', '102'],  # 'ABC' is invalid
        'symbol': ['AAPL', 'AAPL', 'AAPL']
    })

def test_downsample_data_valid(sample_transformer, valid_sample_df):
    """
    Test the _downsample_data method with valid numeric prices.
    """
    transformer = sample_transformer
    df_downsampled = transformer._downsample_data(valid_sample_df)
    
    # Expected downsampled data: each timestamp corresponds to one resample interval
    expected_df = pd.DataFrame({
        'symbol': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
        'timestamp': pd.to_datetime([
            '2024-10-12 09:00:00',
            '2024-10-12 09:01:00',
            '2024-10-12 09:02:00',
            '2024-10-12 09:03:00'
        ]),
        'avg_price': [100.0, 101.0, 102.0, 103.0],
        'median_price': [100.0, 101.0, 102.0, 103.0]
    })
    
    # Reset index for comparison
    df_downsampled_sorted = df_downsampled.sort_values('timestamp').reset_index(drop=True)
    expected_df_sorted = expected_df.sort_values('timestamp').reset_index(drop=True)
    
    pd.testing.assert_frame_equal(df_downsampled_sorted, expected_df_sorted)

def test_downsample_data_multiple_symbols(sample_transformer, valid_sample_df):
    """
    Test the _downsample_data method with multiple symbols in the DataFrame.
    This assumes that fetch_unprocessed_data fetches data for a single symbol as per the transformer's loop.
    """
    transformer = sample_transformer
    # Modify the sample DataFrame to include multiple symbols
    multi_symbol_df = valid_sample_df.copy()
    multi_symbol_df.loc[2, 'symbol'] = 'GOOG'
    
    # Since the transformer's config includes only 'AAPL', the downsampled data should still be for 'AAPL'
    # However, the _downsample_data method assigns 'symbol' based on the first row, which is 'AAPL'
    df_downsampled = transformer._downsample_data(multi_symbol_df)
    
    # Expected downsampled data: all rows are treated as 'AAPL' despite one being 'GOOG'
    expected_df = pd.DataFrame({
        'symbol': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
        'timestamp': pd.to_datetime([
            '2024-10-12 09:00:00',
            '2024-10-12 09:01:00',
            '2024-10-12 09:02:00',
            '2024-10-12 09:03:00'
        ]),
        'avg_price': [100.0, 101.0, 102.0, 103.0],
        'median_price': [100.0, 101.0, 102.0, 103.0]
    })
    
    # Reset index for comparison
    df_downsampled_sorted = df_downsampled.sort_values('timestamp').reset_index(drop=True)
    expected_df_sorted = expected_df.sort_values('timestamp').reset_index(drop=True)
    
    pd.testing.assert_frame_equal(df_downsampled_sorted, expected_df_sorted)

def test_downsample_data_non_uniform_timestamps(sample_transformer):
    """
    Test the _downsample_data method with non-uniform timestamps to see how resampling handles gaps.
    """
    transformer = sample_transformer
    
    # Create a DataFrame with gaps in timestamps
    df = pd.DataFrame({
        'timestamp': pd.to_datetime([
            '2024-10-12 09:00:00',
            '2024-10-12 09:05:00',  # 5-minute gap
            '2024-10-12 09:10:00'
        ]),
        'price': ['100', '105', '110'],
        'symbol': ['AAPL', 'AAPL', 'AAPL']
    })
    
    df_downsampled = transformer._downsample_data(df)
    
    # Expected downsampled data: Each timestamp corresponds to a resample interval, but with gaps, no aggregation needed
    expected_df = pd.DataFrame({
        'symbol': ['AAPL', 'AAPL', 'AAPL'],
        'timestamp': pd.to_datetime([
            '2024-10-12 09:00:00',
            '2024-10-12 09:05:00',
            '2024-10-12 09:10:00'
        ]),
        'avg_price': [100.0, 105.0, 110.0],
        'median_price': [100.0, 105.0, 110.0]
    })
    
    # Reset index for comparison
    df_downsampled_sorted = df_downsampled.sort_values('timestamp').reset_index(drop=True)
    expected_df_sorted = expected_df.sort_values('timestamp').reset_index(drop=True)
    
    pd.testing.assert_frame_equal(df_downsampled_sorted, expected_df_sorted)

def test_downsample_data_duplicate_timestamps(sample_transformer, caplog):
    """
    Test the _downsample_data method with duplicate timestamps to verify aggregation.
    """
    transformer = sample_transformer
    
    # Create a DataFrame with duplicate timestamps
    df = pd.DataFrame({
        'timestamp': pd.to_datetime([
            '2024-10-12 09:00:00',
            '2024-10-12 09:00:00',  # Duplicate
            '2024-10-12 09:01:00',
            '2024-10-12 09:01:00'   # Duplicate
        ]),
        'price': ['100', '110', '101', '111'],
        'symbol': ['AAPL', 'AAPL', 'AAPL', 'AAPL']
    })
    
    df_downsampled = transformer._downsample_data(df)
    
    # Expected downsampled data:
    # 09:00:00: avg_price=(100+110)/2=105, median_price=105
    # 09:01:00: avg_price=(101+111)/2=106, median_price=106
    expected_df = pd.DataFrame({
        'symbol': ['AAPL', 'AAPL'],
        'timestamp': pd.to_datetime([
            '2024-10-12 09:00:00',
            '2024-10-12 09:01:00'
        ]),
        'avg_price': [105.0, 106.0],
        'median_price': [105.0, 106.0]
    })
    
    # Reset index for comparison
    df_downsampled_sorted = df_downsampled.sort_values('timestamp').reset_index(drop=True)
    expected_df_sorted = expected_df.sort_values('timestamp').reset_index(drop=True)
    
    pd.testing.assert_frame_equal(df_downsampled_sorted, expected_df_sorted)
