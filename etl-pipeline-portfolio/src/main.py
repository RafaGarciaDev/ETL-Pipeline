"""
Main ETL Pipeline Orchestrator
Coordinates Extract, Transform, and Load processes.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add src to path
sys.path.append(str(Path(__file__).parent))

from extract.api_extractor import APIExtractor
from extract.csv_extractor import CSVExtractor
from transform.transformer import DataTransformer
from load.db_loader import DatabaseLoader
from utils.config import Config
from utils.logger import setup_logger


class ETLPipeline:
    """
    Main ETL Pipeline orchestrator.
    
    Coordinates the full ETL process:
    1. Extract data from multiple sources
    2. Transform and clean data
    3. Load into data warehouse
    """
    
    def __init__(self, config_path: str = 'config/pipeline.yaml'):
        """
        Initialize ETL Pipeline.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = Config(config_path)
        self.logger = setup_logger(
            name='ETLPipeline',
            log_file=f'data/logs/etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
        
        self.logger.info("=" * 80)
        self.logger.info("ETL Pipeline Starting")
        self.logger.info("=" * 80)
    
    def run(self, source: Optional[str] = None) -> bool:
        """
        Run complete ETL pipeline.
        
        Args:
            source: Specific source to process (None for all)
            
        Returns:
            True if successful
        """
        try:
            start_time = datetime.now()
            self.logger.info(f"Pipeline start time: {start_time}")
            
            # Extract
            self.logger.info("STEP 1: EXTRACT")
            raw_data = self.extract(source)
            
            if raw_data is None or raw_data.empty:
                self.logger.warning("No data extracted. Aborting pipeline.")
                return False
            
            # Transform
            self.logger.info("STEP 2: TRANSFORM")
            transformed_data = self.transform(raw_data)
            
            # Load
            self.logger.info("STEP 3: LOAD")
            rows_loaded = self.load(transformed_data)
            
            # Summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.logger.info("=" * 80)
            self.logger.info("PIPELINE SUMMARY")
            self.logger.info(f"Status: SUCCESS")
            self.logger.info(f"Rows Processed: {len(transformed_data)}")
            self.logger.info(f"Rows Loaded: {rows_loaded}")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return False
    
    def extract(self, source: Optional[str] = None):
        """
        Extract data from configured sources.
        
        Args:
            source: Specific source to extract (None for all)
            
        Returns:
            Extracted data as DataFrame
        """
        import pandas as pd
        
        all_data = []
        
        # Extract from CSV
        if source in [None, 'csv']:
            try:
                self.logger.info("Extracting from CSV files...")
                csv_extractor = CSVExtractor()
                
                csv_files = self.config.get('extract.csv.files', [])
                for file_path in csv_files:
                    df = csv_extractor.extract(file_path)
                    df['source'] = 'csv'
                    df['source_file'] = Path(file_path).name
                    all_data.append(df)
                
                self.logger.info(f"CSV extraction completed: {len(csv_files)} files")
                
            except Exception as e:
                self.logger.error(f"CSV extraction failed: {e}")
        
        # Extract from API
        if source in [None, 'api']:
            try:
                self.logger.info("Extracting from API...")
                api_config = self.config.get('extract.api', {})
                
                api_extractor = APIExtractor(
                    base_url=api_config.get('base_url'),
                    api_key=api_config.get('api_key')
                )
                
                endpoint = api_config.get('endpoint')
                params = api_config.get('params', {})
                
                data = api_extractor.extract(endpoint, params)
                df = pd.DataFrame(data)
                df['source'] = 'api'
                all_data.append(df)
                
                self.logger.info(f"API extraction completed: {len(df)} records")
                
            except Exception as e:
                self.logger.error(f"API extraction failed: {e}")
        
        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"Total records extracted: {len(combined_df)}")
            
            # Save raw data
            raw_path = f"data/raw/raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            combined_df.to_csv(raw_path, index=False)
            self.logger.info(f"Raw data saved to: {raw_path}")
            
            return combined_df
        else:
            return None
    
    def transform(self, df):
        """
        Transform and clean data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        transformer = DataTransformer()
        
        # Clean data
        self.logger.info("Cleaning data...")
        df = transformer.clean(
            df,
            remove_duplicates=True,
            handle_nulls='fill'
        )
        
        # Standardize columns
        self.logger.info("Standardizing columns...")
        df = transformer.standardize_columns(
            df,
            to_lowercase=True,
            replace_spaces=True
        )
        
        # Convert types
        type_map = self.config.get('transform.type_conversions', {})
        if type_map:
            self.logger.info("Converting data types...")
            df = transformer.convert_types(df, type_map)
        
        # Add metadata columns
        df['etl_loaded_at'] = datetime.now()
        df['etl_batch_id'] = datetime.now().strftime('%Y%m%d%H%M%S')
        
        self.logger.info(f"Transformation completed: {len(df)} rows")
        
        # Save processed data
        processed_path = f"data/processed/processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(processed_path, index=False)
        self.logger.info(f"Processed data saved to: {processed_path}")
        
        return df
    
    def load(self, df):
        """
        Load data into database.
        
        Args:
            df: DataFrame to load
            
        Returns:
            Number of rows loaded
        """
        db_config = self.config.get('load.database', {})
        
        loader = DatabaseLoader(
            connection_string=db_config.get('connection_string'),
            schema=db_config.get('schema', 'public')
        )
        
        table_name = db_config.get('table_name', 'etl_data')
        strategy = db_config.get('strategy', 'append')
        
        self.logger.info(f"Loading to table: {table_name}")
        
        rows_loaded = loader.load(
            df,
            table_name=table_name,
            strategy=strategy,
            batch_size=1000
        )
        
        self.logger.info(f"Load completed: {rows_loaded} rows")
        
        return rows_loaded


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Pipeline')
    parser.add_argument(
        '--source',
        choices=['csv', 'api', 'all'],
        default='all',
        help='Data source to process'
    )
    parser.add_argument(
        '--config',
        default='config/pipeline.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = ETLPipeline(config_path=args.config)
    source = None if args.source == 'all' else args.source
    
    success = pipeline.run(source=source)
    
    if success:
        print("\n✅ Pipeline completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Pipeline failed. Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
