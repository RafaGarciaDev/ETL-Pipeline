"""
Data Transformer Module
Applies cleaning, validation, and transformations to data.
"""

import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Optional, Callable
from datetime import datetime

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transform and clean data with comprehensive operations.
    
    Features:
    - Data cleaning (nulls, duplicates)
    - Type conversion
    - Column renaming and standardization
    - Derived columns
    - Data validation
    """
    
    def __init__(self):
        """Initialize Data Transformer."""
        logger.info("DataTransformer initialized")
    
    def clean(
        self,
        df: pd.DataFrame,
        remove_duplicates: bool = True,
        handle_nulls: str = 'drop',
        null_threshold: float = 0.5
    ) -> pd.DataFrame:
        """
        Clean DataFrame.
        
        Args:
            df: Input DataFrame
            remove_duplicates: Remove duplicate rows
            handle_nulls: How to handle nulls ('drop', 'fill', 'keep')
            null_threshold: Max proportion of nulls allowed per column
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Starting data cleaning")
        initial_rows = len(df)
        
        # Remove duplicates
        if remove_duplicates:
            df = df.drop_duplicates()
            logger.info(f"Removed {initial_rows - len(df)} duplicate rows")
        
        # Handle columns with too many nulls
        null_counts = df.isnull().sum() / len(df)
        cols_to_drop = null_counts[null_counts > null_threshold].index.tolist()
        
        if cols_to_drop:
            logger.warning(f"Dropping columns with >{null_threshold:.0%} nulls: {cols_to_drop}")
            df = df.drop(columns=cols_to_drop)
        
        # Handle remaining nulls
        if handle_nulls == 'drop':
            df = df.dropna()
            logger.info(f"Dropped rows with nulls. Remaining: {len(df)}")
        elif handle_nulls == 'fill':
            df = self._fill_nulls(df)
        
        logger.info(f"Cleaning completed. Final rows: {len(df)}")
        return df
    
    def _fill_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill null values intelligently based on data type.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with nulls filled
        """
        for col in df.columns:
            if df[col].isnull().any():
                if df[col].dtype in ['int64', 'float64']:
                    # Fill numeric with median
                    df[col].fillna(df[col].median(), inplace=True)
                elif df[col].dtype == 'object':
                    # Fill categorical with mode or 'Unknown'
                    mode_val = df[col].mode()
                    fill_val = mode_val[0] if len(mode_val) > 0 else 'Unknown'
                    df[col].fillna(fill_val, inplace=True)
                elif df[col].dtype == 'datetime64[ns]':
                    # Fill dates with median date
                    df[col].fillna(df[col].median(), inplace=True)
        
        return df
    
    def standardize_columns(
        self,
        df: pd.DataFrame,
        rename_map: Optional[Dict[str, str]] = None,
        to_lowercase: bool = True,
        replace_spaces: bool = True
    ) -> pd.DataFrame:
        """
        Standardize column names.
        
        Args:
            df: Input DataFrame
            rename_map: Dictionary mapping old to new names
            to_lowercase: Convert to lowercase
            replace_spaces: Replace spaces with underscores
            
        Returns:
            DataFrame with standardized columns
        """
        logger.info("Standardizing column names")
        
        # Apply rename map
        if rename_map:
            df = df.rename(columns=rename_map)
        
        # Standardize format
        new_columns = []
        for col in df.columns:
            new_col = col
            if to_lowercase:
                new_col = new_col.lower()
            if replace_spaces:
                new_col = new_col.replace(' ', '_')
            new_columns.append(new_col)
        
        df.columns = new_columns
        logger.info(f"Columns standardized: {df.columns.tolist()}")
        
        return df
    
    def convert_types(
        self,
        df: pd.DataFrame,
        type_map: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Convert column data types.
        
        Args:
            df: Input DataFrame
            type_map: Dictionary mapping column names to target types
            
        Returns:
            DataFrame with converted types
        """
        logger.info("Converting data types")
        
        for col, dtype in type_map.items():
            if col not in df.columns:
                logger.warning(f"Column {col} not found, skipping")
                continue
            
            try:
                if dtype == 'datetime':
                    df[col] = pd.to_datetime(df[col])
                elif dtype == 'category':
                    df[col] = df[col].astype('category')
                elif dtype in ['int', 'int64']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif dtype in ['float', 'float64']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                else:
                    df[col] = df[col].astype(dtype)
                
                logger.info(f"Converted {col} to {dtype}")
                
            except Exception as e:
                logger.error(f"Failed to convert {col} to {dtype}: {e}")
        
        return df
    
    def add_derived_columns(
        self,
        df: pd.DataFrame,
        derivations: Dict[str, Callable]
    ) -> pd.DataFrame:
        """
        Add derived columns using custom functions.
        
        Args:
            df: Input DataFrame
            derivations: Dict mapping new column names to functions
            
        Returns:
            DataFrame with derived columns
        """
        logger.info(f"Adding {len(derivations)} derived columns")
        
        for col_name, func in derivations.items():
            try:
                df[col_name] = func(df)
                logger.info(f"Added derived column: {col_name}")
            except Exception as e:
                logger.error(f"Failed to create {col_name}: {e}")
        
        return df
    
    def aggregate(
        self,
        df: pd.DataFrame,
        group_by: List[str],
        agg_functions: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Aggregate data by groups.
        
        Args:
            df: Input DataFrame
            group_by: Columns to group by
            agg_functions: Dict mapping columns to aggregation functions
            
        Returns:
            Aggregated DataFrame
        """
        logger.info(f"Aggregating by {group_by}")
        
        try:
            df_agg = df.groupby(group_by).agg(agg_functions).reset_index()
            
            # Flatten column names if multi-level
            if isinstance(df_agg.columns, pd.MultiIndex):
                df_agg.columns = ['_'.join(col).strip('_') for col in df_agg.columns]
            
            logger.info(f"Aggregation completed. Rows: {len(df_agg)}")
            return df_agg
            
        except Exception as e:
            logger.error(f"Aggregation failed: {e}")
            raise
    
    def validate_data(
        self,
        df: pd.DataFrame,
        validations: Dict[str, Callable]
    ) -> Dict[str, bool]:
        """
        Validate data using custom validation functions.
        
        Args:
            df: DataFrame to validate
            validations: Dict mapping validation names to functions
            
        Returns:
            Dict of validation results
        """
        logger.info("Running data validations")
        results = {}
        
        for name, func in validations.items():
            try:
                result = func(df)
                results[name] = result
                
                if result:
                    logger.info(f"✓ Validation passed: {name}")
                else:
                    logger.warning(f"✗ Validation failed: {name}")
                    
            except Exception as e:
                logger.error(f"Validation error in {name}: {e}")
                results[name] = False
        
        return results
    
    def enrich_with_lookup(
        self,
        df: pd.DataFrame,
        lookup_df: pd.DataFrame,
        on: str,
        how: str = 'left'
    ) -> pd.DataFrame:
        """
        Enrich data with lookup table.
        
        Args:
            df: Main DataFrame
            lookup_df: Lookup DataFrame
            on: Column to join on
            how: Join type
            
        Returns:
            Enriched DataFrame
        """
        logger.info(f"Enriching data with lookup on '{on}'")
        
        initial_rows = len(df)
        df_enriched = df.merge(lookup_df, on=on, how=how)
        
        if len(df_enriched) != initial_rows and how == 'left':
            logger.warning(
                f"Row count changed after join: {initial_rows} -> {len(df_enriched)}"
            )
        
        logger.info("Enrichment completed")
        return df_enriched


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example transformation
    transformer = DataTransformer()
    
    # Sample data
    df = pd.DataFrame({
        'Date': ['2024-01-01', '2024-01-02', '2024-01-01'],
        'Product': ['A', 'B', 'A'],
        'Quantity': [10, 20, 10],
        'Revenue': [100.0, 200.0, 100.0]
    })
    
    # Clean
    df = transformer.clean(df, remove_duplicates=True)
    
    # Convert types
    df = transformer.convert_types(df, {'Date': 'datetime'})
    
    # Add derived column
    df = transformer.add_derived_columns(
        df,
        {'unit_price': lambda x: x['Revenue'] / x['Quantity']}
    )
    
    print(df)
