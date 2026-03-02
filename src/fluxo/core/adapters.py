from abc import ABC, abstractmethod
from typing import Any, List, Dict, Tuple, Optional
import json

class BaseAdapter(ABC):
    """
    Interface for database adapters handling SQL execution and profiling.
    """
    
    @abstractmethod
    def execute(self, query: str) -> None:
        """Executes a SQL query without returning results."""
        ...

    @abstractmethod
    def fetchone(self, query: str) -> Tuple:
        """Executes a SQL query and returns the first row."""
        ...
        
    @abstractmethod
    def fetchall(self, query: str) -> List[Tuple]:
        """Executes a SQL query and returns all rows."""
        ...

    @abstractmethod
    def get_row_count(self, table_name: str) -> int:
        """Returns the number of rows in a table."""
        ...

    @abstractmethod
    def get_profiling_stats(self, table_name: str, numeric_columns: List[str]) -> Dict[str, Any]:
        """
        Returns profiling stats (min, max, mean, null_count) for the specified numeric columns in a table.
        """
        ...

    @abstractmethod
    def close(self):
        """Closes the connection to the database."""
        pass

class DuckDBAdapter(BaseAdapter):
    def __init__(self, db_path: str = "fluxo_target.duckdb", memory: bool = False):
        import duckdb # type: ignore
        self.conn = duckdb.connect(":memory:" if memory else db_path)
        
    def execute(self, query: str) -> None:
        self.conn.execute(query)
        
    def fetchone(self, query: str) -> Tuple:
        return self.conn.execute(query).fetchone()
        
    def fetchall(self, query: str) -> List[Tuple]:
        return self.conn.execute(query).fetchall()

    def get_row_count(self, table_name: str) -> int:
        try:
            return self.fetchone(f"SELECT count(*) FROM {table_name}")[0]
        except Exception:
            return 0

    def get_profiling_stats(self, table_name: str, numeric_columns: List[str]) -> Dict[str, Any]:
        stats = {}
        for col in numeric_columns:
            try:
                query = f"""
                SELECT 
                    MIN({col}), 
                    MAX({col}), 
                    AVG({col}), 
                    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) 
                FROM {table_name}
                """
                res = self.fetchone(query)
                if res and res[0] is not None:
                    stats[col] = {
                        "min": float(res[0]),
                        "max": float(res[1]),
                        "mean": round(float(res[2]), 2), # type: ignore
                        "null_count": int(res[3])
                    }
            except Exception:
                pass
        return stats

    def close(self):
        self.conn.close()

class PostgresAdapter(BaseAdapter):
    def __init__(self, **kwargs):
        try:
            import psycopg2 # type: ignore
            self.conn = psycopg2.connect(**kwargs)
            self.conn.autocommit = True
        except ImportError:
            raise ImportError("psycopg2 is required for PostgresAdapter. Run: pip install psycopg2-binary")
            
    def execute(self, query: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(query)
            
    def fetchone(self, query: str) -> Tuple:
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()
            
    def fetchall(self, query: str) -> List[Tuple]:
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def get_row_count(self, table_name: str) -> int:
        try:
            res = self.fetchone(f"SELECT count(*) FROM {table_name}")
            return res[0] if res else 0
        except Exception:
            return 0

    def get_profiling_stats(self, table_name: str, numeric_columns: List[str]) -> Dict[str, Any]:
        stats = {}
        for col in numeric_columns:
            try:
                query = f"""
                SELECT 
                    MIN({col}), 
                    MAX({col}), 
                    AVG({col}), 
                    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) 
                FROM {table_name}
                """
                res = self.fetchone(query)
                if res and res[0] is not None:
                    stats[col] = {
                        "min": float(res[0]),
                        "max": float(res[1]),
                        "mean": round(float(res[2]), 2) if res[2] else 0.0, # type: ignore
                        "null_count": int(res[3]) if res[3] else 0
                    }
            except Exception:
                pass
        return stats

    def close(self):
        self.conn.close()

class BigQueryAdapter(BaseAdapter):
    def __init__(self, **kwargs):
        try:
            from google.cloud import bigquery # type: ignore
            # BigQuery kwargs might include project, credentials, etc...
            self.client = bigquery.Client(**kwargs)
        except ImportError:
            raise ImportError("google-cloud-bigquery is required for BigQueryAdapter. Run: pip install google-cloud-bigquery")

    def execute(self, query: str) -> None:
        job = self.client.query(query)
        job.result() # Wait for job to complete

    def fetchone(self, query: str) -> Tuple:
        job = self.client.query(query)
        result = list(job.result())
        if result:
            return tuple(result[0].values())
        return ()

    def fetchall(self, query: str) -> List[Tuple]:
        job = self.client.query(query)
        return [tuple(row.values()) for row in job.result()]

    def get_row_count(self, table_name: str) -> int:
        try:
            res = self.fetchone(f"SELECT count(*) FROM {table_name}")
            return res[0] if res else 0
        except Exception:
            return 0

    def get_profiling_stats(self, table_name: str, numeric_columns: List[str]) -> Dict[str, Any]:
        stats = {}
        for col in numeric_columns:
            try:
                query = f"""
                SELECT 
                    MIN({col}), 
                    MAX({col}), 
                    AVG({col}), 
                    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) 
                FROM {table_name}
                """
                res = self.fetchone(query)
                if res and res[0] is not None:
                    stats[col] = {
                        "min": float(res[0]),
                        "max": float(res[1]),
                        "mean": round(float(res[2]), 2) if res[2] else 0.0, # type: ignore
                        "null_count": int(res[3]) if res[3] else 0
                    }
            except Exception:
                pass
        return stats

    def close(self):
        # BigQuery client handles connection pooling, explicit close isn't strictly required
        self.client.close()
