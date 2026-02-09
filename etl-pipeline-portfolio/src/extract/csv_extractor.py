"""
CSV Extractor Module
Extracts data from CSV files with validation and error handling.
"""

import pandas as pd
import logging
from typing import List, Optional, Dict
from pathlib import Path
import chardet

logger = logging.getLogger(__name__)


class CSVExtractor:
    """
    Extract data from CSV files with robust error handling.
    
    Features:
    - Automatic encoding detection
    - Schema validation
    - Chunked reading for large files
    - Multiple delimiter support
    """
    
    def __init__(
        self,
        delimiter: str = ',',
        encoding: Optional[str] = None,
        chunk_size: Optional[int] = None
    ):
        """
        Initialize CSV Extractor.
        
        Args:
            delimiter: CSV delimiter (default: comma)
            encoding: File encoding (auto-detected if None)
            chunk_size: Number of rows to read at once (None for all)
        """
        self.delimiter = delimiter
        self.encoding = encoding
        self.chunk_size = chunk_size
        
        logger.info("CSVExtractor initialized")
    
    def extract(
        self,
        file_path: str,
        columns: Optional[List[str]] = None,
        dtypes: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            file_path: Path to CSV file
            columns: Specific columns to extract (None for all)
            dtypes: Data types for columns
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Starting extraction from {file_path}")
        
        # Validate file exists
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Detect encoding if not specified
        encoding = self.encoding or self._detect_encoding(file_path)
        
        try:
            # Read CSV
            if self.chunk_size:
                df = self._read_in_chunks(
                    file_path, encoding, columns, dtypes
                )
            else:
                df = pd.read_csv(
                    file_path,
                    delimiter=self.delimiter,
                    encoding=encoding,
                    usecols=columns,
                    dtype=dtypes,
                    low_memory=False
                )
            
            logger.info(
                f"Extraction completed. "
                f"Rows: {len(df)}, Columns: {len(df.columns)}"
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _detect_encoding(self, file_path: str) -> str:
        """
        Detect file encoding.
        
        Args:
            file_path: Path to file
            
        Returns:
            Detected encoding
        """
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read(10000))
        
        encoding = result['encoding']
        confidence = result['confidence']
        
        logger.info(
            f"Detected encoding: {encoding} "
            f"(confidence: {confidence:.2%})"
        )
        
        return encoding
    
    def _read_in_chunks(
        self,
        file_path: str,
        encoding: str,
        columns: Optional[List[str]],
        dtypes: Optional[Dict]
    ) -> pd.DataFrame:
        """
        Read large CSV in chunks.
        
        Args:
            file_path: Path to CSV file
            encoding: File encoding
            columns: Columns to extract
            dtypes: Data types
            
        Returns:
            Combined DataFrame
        """
        logger.info(f"Reading file in chunks of {self.chunk_size} rows")
        
        chunks = []
        
        for chunk in pd.read_csv(
            file_path,
            delimiter=self.delimiter,
            encoding=encoding,
            usecols=columns,
            dtype=dtypes,
            chunksize=self.chunk_size,
            low_memory=False
        ):
            chunks.append(chunk)
        
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f"Combined {len(chunks)} chunks")
        
        return df
    
    def extract_multiple(
        self,
        file_paths: List[str],
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Extract and combine data from multiple CSV files.
        
        Args:
            file_paths: List of CSV file paths
            columns: Columns to extract
            
        Returns:
            Combined DataFrame
        """
        logger.info(f"Extracting from {len(file_paths)} files")
        
        dfs = []
        
        for file_path in file_paths:
            try:
                df = self.extract(file_path, columns)
                df['source_file'] = Path(file_path).name
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to extract {file_path}: {e}")
                continue
        
        if not dfs:
            raise ValueError("No files were successfully extracted")
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        logger.info(
            f"Combined extraction completed. "
            f"Total rows: {len(combined_df)}"
        )
        
        return combined_df
    
    def validate_schema(
        self,
        df: pd.DataFrame,
        expected_columns: List[str]
    ) -> bool:
        """
        Validate DataFrame schema.
        
        Args:
            df: DataFrame to validate
            expected_columns: Expected column names
            
        Returns:
            True if schema is valid
        """
        actual_columns = set(df.columns)
        expected_columns = set(expected_columns)
        
        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns
        
        if missing:
            logger.error(f"Missing columns: {missing}")
            return False
        
        if extra:
            logger.warning(f"Extra columns found: {extra}")
        
        logger.info("Schema validation passed")
        return True
    
    def get_file_info(self, file_path: str) -> Dict:
        """
        Get information about CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Dictionary with file information
        """
        path = Path(file_path)
        
        # Quick read to get column info
        df_sample = pd.read_csv(file_path, nrows=5)
        
        # Count total rows
        with open(file_path, 'r') as f:
            row_count = sum(1 for _ in f) - 1  # -1 for header
        
        info = {
            'file_name': path.name,
            'file_size_mb': path.stat().st_size / (1024 * 1024),
            'row_count': row_count,
            'column_count': len(df_sample.columns),
            'columns': df_sample.columns.tolist(),
            'dtypes': df_sample.dtypes.to_dict()
        }
        
        logger.info(f"File info: {info}")
        return info


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example: Extract sales data
    extractor = CSVExtractor(delimiter=',')
    
    df = extractor.extract(
        file_path='data/raw/sales.csv',
        columns=['date', 'product', 'quantity', 'revenue']
    )
    
    print(f"Extracted {len(df)} rows")
    print(df.head())
