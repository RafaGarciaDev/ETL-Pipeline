"""
Unit tests for extraction modules.
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / 'src'))

from extract.csv_extractor import CSVExtractor
from extract.api_extractor import APIExtractor


class TestCSVExtractor:
    """Test CSV Extractor functionality."""
    
    def test_extract_basic(self, tmp_path):
        """Test basic CSV extraction."""
        # Create test CSV
        test_file = tmp_path / "test.csv"
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [100, 200, 300]
        })
        test_data.to_csv(test_file, index=False)
        
        # Extract
        extractor = CSVExtractor()
        result = extractor.extract(str(test_file))
        
        # Assert
        assert len(result) == 3
        assert list(result.columns) == ['id', 'name', 'value']
        assert result['id'].tolist() == [1, 2, 3]
    
    def test_extract_specific_columns(self, tmp_path):
        """Test extraction with column selection."""
        # Create test CSV
        test_file = tmp_path / "test.csv"
        test_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['A', 'B'],
            'value': [100, 200]
        })
        test_data.to_csv(test_file, index=False)
        
        # Extract specific columns
        extractor = CSVExtractor()
        result = extractor.extract(str(test_file), columns=['id', 'name'])
        
        # Assert
        assert list(result.columns) == ['id', 'name']
        assert 'value' not in result.columns
    
    def test_extract_file_not_found(self):
        """Test handling of non-existent file."""
        extractor = CSVExtractor()
        
        with pytest.raises(FileNotFoundError):
            extractor.extract('nonexistent.csv')
    
    def test_validate_schema(self):
        """Test schema validation."""
        extractor = CSVExtractor()
        
        df = pd.DataFrame({
            'id': [1, 2],
            'name': ['A', 'B']
        })
        
        # Valid schema
        assert extractor.validate_schema(df, ['id', 'name']) == True
        
        # Missing column
        assert extractor.validate_schema(df, ['id', 'name', 'missing']) == False


class TestAPIExtractor:
    """Test API Extractor functionality."""
    
    def test_init(self):
        """Test initialization."""
        extractor = APIExtractor(
            base_url="https://api.example.com",
            api_key="test_key"
        )
        
        assert extractor.base_url == "https://api.example.com"
        assert extractor.api_key == "test_key"
        assert extractor.max_retries == 3
    
    def test_parse_response_list(self):
        """Test parsing list response."""
        from unittest.mock import Mock
        
        extractor = APIExtractor("https://api.example.com")
        
        # Mock response with list
        response = Mock()
        response.json.return_value = [
            {'id': 1, 'name': 'A'},
            {'id': 2, 'name': 'B'}
        ]
        
        result = extractor._parse_response(response)
        
        assert len(result) == 2
        assert result[0]['id'] == 1
    
    def test_parse_response_dict_with_data(self):
        """Test parsing dict response with 'data' key."""
        from unittest.mock import Mock
        
        extractor = APIExtractor("https://api.example.com")
        
        # Mock response with dict containing 'data'
        response = Mock()
        response.json.return_value = {
            'data': [
                {'id': 1, 'name': 'A'},
                {'id': 2, 'name': 'B'}
            ],
            'meta': {'total': 2}
        }
        
        result = extractor._parse_response(response)
        
        assert len(result) == 2
        assert result[0]['id'] == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
