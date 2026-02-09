"""
Unit tests for transformation module.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / 'src'))

from transform.transformer import DataTransformer


class TestDataTransformer:
    """Test Data Transformer functionality."""
    
    def test_clean_remove_duplicates(self):
        """Test duplicate removal."""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'value': [100, 200, 200, 300]
        })
        
        result = transformer.clean(df, remove_duplicates=True, handle_nulls='keep')
        
        assert len(result) == 3
        assert result['id'].tolist() == [1, 2, 3]
    
    def test_clean_drop_nulls(self):
        """Test null dropping."""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'id': [1, 2, None, 4],
            'value': [100, 200, 300, 400]
        })
        
        result = transformer.clean(df, remove_duplicates=False, handle_nulls='drop')
        
        assert len(result) == 3
        assert result['id'].isna().sum() == 0
    
    def test_standardize_columns(self):
        """Test column standardization."""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'Customer ID': [1, 2],
            'Full Name': ['John', 'Jane'],
            'Email Address': ['john@test.com', 'jane@test.com']
        })
        
        result = transformer.standardize_columns(
            df,
            to_lowercase=True,
            replace_spaces=True
        )
        
        expected_columns = ['customer_id', 'full_name', 'email_address']
        assert result.columns.tolist() == expected_columns
    
    def test_convert_types(self):
        """Test type conversion."""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'amount': ['100.50', '200.75'],
            'count': ['10', '20']
        })
        
        type_map = {
            'date': 'datetime',
            'amount': 'float64',
            'count': 'int64'
        }
        
        result = transformer.convert_types(df, type_map)
        
        assert pd.api.types.is_datetime64_any_dtype(result['date'])
        assert pd.api.types.is_float_dtype(result['amount'])
        assert pd.api.types.is_integer_dtype(result['count'])
    
    def test_add_derived_columns(self):
        """Test adding derived columns."""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'quantity': [10, 20],
            'price': [100.0, 200.0]
        })
        
        derivations = {
            'total': lambda x: x['quantity'] * x['price']
        }
        
        result = transformer.add_derived_columns(df, derivations)
        
        assert 'total' in result.columns
        assert result['total'].tolist() == [1000.0, 4000.0]
    
    def test_aggregate(self):
        """Test data aggregation."""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'category': ['A', 'A', 'B', 'B'],
            'value': [10, 20, 30, 40]
        })
        
        result = transformer.aggregate(
            df,
            group_by=['category'],
            agg_functions={'value': 'sum'}
        )
        
        assert len(result) == 2
        assert result[result['category'] == 'A']['value'].values[0] == 30
        assert result[result['category'] == 'B']['value'].values[0] == 70
    
    def test_validate_data(self):
        """Test data validation."""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })
        
        validations = {
            'has_id': lambda x: 'id' in x.columns,
            'positive_values': lambda x: (x['value'] > 0).all(),
            'no_nulls': lambda x: x.isna().sum().sum() == 0
        }
        
        results = transformer.validate_data(df, validations)
        
        assert results['has_id'] == True
        assert results['positive_values'] == True
        assert results['no_nulls'] == True
    
    def test_enrich_with_lookup(self):
        """Test data enrichment with lookup."""
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })
        
        lookup_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })
        
        result = transformer.enrich_with_lookup(df, lookup_df, on='id')
        
        assert 'name' in result.columns
        assert len(result) == 3
        assert result['name'].tolist() == ['A', 'B', 'C']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
