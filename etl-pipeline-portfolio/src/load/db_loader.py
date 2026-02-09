"""
Database Loader Module
Loads data into PostgreSQL with different strategies.
"""

import pandas as pd
import logging
from typing import Optional, List
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
import time

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """
    Load data into PostgreSQL database.
    
    Features:
    - Multiple load strategies (append, replace, upsert)
    - Batch processing
    - Transaction management
    - Performance optimization
    """
    
    def __init__(
        self,
        connection_string: str,
        schema: str = 'public'
    ):
        """
        Initialize Database Loader.
        
        Args:
            connection_string: SQLAlchemy connection string
            schema: Database schema name
        """
        self.connection_string = connection_string
        self.schema = schema
        self.engine = create_engine(connection_string)
        
        logger.info(f"DatabaseLoader initialized for schema: {schema}")
    
    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        strategy: str = 'append',
        batch_size: int = 1000,
        create_index: Optional[List[str]] = None
    ) -> int:
        """
        Load DataFrame into database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            strategy: Load strategy ('append', 'replace', 'upsert')
            batch_size: Number of rows per batch
            create_index: Columns to create index on
            
        Returns:
            Number of rows loaded
        """
        logger.info(
            f"Loading {len(df)} rows to {self.schema}.{table_name} "
            f"(strategy: {strategy})"
        )
        
        start_time = time.time()
        
        try:
            if strategy == 'append':
                rows_loaded = self._load_append(df, table_name, batch_size)
            elif strategy == 'replace':
                rows_loaded = self._load_replace(df, table_name, batch_size)
            elif strategy == 'upsert':
                rows_loaded = self._load_upsert(df, table_name, batch_size)
            else:
                raise ValueError(f"Unknown strategy: {strategy}")
            
            # Create indexes if specified
            if create_index:
                self._create_indexes(table_name, create_index)
            
            duration = time.time() - start_time
            logger.info(
                f"Load completed. Rows: {rows_loaded}, "
                f"Duration: {duration:.2f}s"
            )
            
            return rows_loaded
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise
    
    def _load_append(
        self,
        df: pd.DataFrame,
        table_name: str,
        batch_size: int
    ) -> int:
        """
        Load data by appending to existing table.
        
        Args:
            df: DataFrame to load
            table_name: Target table
            batch_size: Batch size
            
        Returns:
            Rows loaded
        """
        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False,
            chunksize=batch_size,
            method='multi'
        )
        
        return len(df)
    
    def _load_replace(
        self,
        df: pd.DataFrame,
        table_name: str,
        batch_size: int
    ) -> int:
        """
        Load data by replacing existing table.
        
        Args:
            df: DataFrame to load
            table_name: Target table
            batch_size: Batch size
            
        Returns:
            Rows loaded
        """
        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='replace',
            index=False,
            chunksize=batch_size,
            method='multi'
        )
        
        return len(df)
    
    def _load_upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        batch_size: int,
        key_columns: Optional[List[str]] = None
    ) -> int:
        """
        Load data with upsert (insert or update).
        
        Args:
            df: DataFrame to load
            table_name: Target table
            batch_size: Batch size
            key_columns: Columns to use for matching (default: first column)
            
        Returns:
            Rows loaded
        """
        if key_columns is None:
            key_columns = [df.columns[0]]
        
        # Create temporary table
        temp_table = f"{table_name}_temp"
        df.to_sql(
            name=temp_table,
            con=self.engine,
            schema=self.schema,
            if_exists='replace',
            index=False
        )
        
        # Build upsert query
        columns = df.columns.tolist()
        set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in key_columns])
        
        upsert_query = f"""
        INSERT INTO {self.schema}.{table_name} ({', '.join(columns)})
        SELECT {', '.join(columns)}
        FROM {self.schema}.{temp_table}
        ON CONFLICT ({', '.join(key_columns)})
        DO UPDATE SET {set_clause}
        """
        
        with self.engine.begin() as conn:
            result = conn.execute(text(upsert_query))
            conn.execute(text(f"DROP TABLE {self.schema}.{temp_table}"))
        
        return len(df)
    
    def _create_indexes(
        self,
        table_name: str,
        columns: List[str]
    ) -> None:
        """
        Create indexes on specified columns.
        
        Args:
            table_name: Table name
            columns: Columns to index
        """
        logger.info(f"Creating indexes on {columns}")
        
        with self.engine.begin() as conn:
            for col in columns:
                index_name = f"idx_{table_name}_{col}"
                query = f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON {self.schema}.{table_name} ({col})
                """
                conn.execute(text(query))
        
        logger.info("Indexes created successfully")
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.
        
        Args:
            table_name: Table name to check
            
        Returns:
            True if table exists
        """
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names(schema=self.schema)
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get number of rows in table.
        
        Args:
            table_name: Table name
            
        Returns:
            Row count
        """
        query = f"SELECT COUNT(*) FROM {self.schema}.{table_name}"
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            count = result.scalar()
        
        return count
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results.
        
        Args:
            query: SQL query
            
        Returns:
            Query results as DataFrame
        """
        return pd.read_sql(query, self.engine)
    
    def truncate_table(self, table_name: str) -> None:
        """
        Truncate table (delete all rows).
        
        Args:
            table_name: Table to truncate
        """
        logger.warning(f"Truncating table: {self.schema}.{table_name}")
        
        with self.engine.begin() as conn:
            conn.execute(
                text(f"TRUNCATE TABLE {self.schema}.{table_name}")
            )
        
        logger.info("Table truncated successfully")
    
    def create_table_from_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_key: Optional[str] = None
    ) -> None:
        """
        Create table from DataFrame schema.
        
        Args:
            df: DataFrame with desired schema
            table_name: Name for new table
            primary_key: Column to use as primary key
        """
        logger.info(f"Creating table: {self.schema}.{table_name}")
        
        # Create table
        df.head(0).to_sql(
            name=table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='replace',
            index=False
        )
        
        # Add primary key if specified
        if primary_key:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE {self.schema}.{table_name} "
                        f"ADD PRIMARY KEY ({primary_key})"
                    )
                )
        
        logger.info("Table created successfully")
    
    def get_last_updated(
        self,
        table_name: str,
        date_column: str
    ) -> Optional[pd.Timestamp]:
        """
        Get the latest date from a table.
        
        Args:
            table_name: Table name
            date_column: Date column to check
            
        Returns:
            Latest timestamp or None
        """
        query = f"""
        SELECT MAX({date_column}) as max_date
        FROM {self.schema}.{table_name}
        """
        
        try:
            result = pd.read_sql(query, self.engine)
            return result['max_date'].iloc[0]
        except:
            return None


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example connection
    conn_string = "postgresql://user:password@localhost:5432/database"
    loader = DatabaseLoader(conn_string, schema='etl')
    
    # Sample data
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [100, 200, 300]
    })
    
    # Load data
    loader.load(
        df,
        table_name='sample_table',
        strategy='replace',
        create_index=['id']
    )
