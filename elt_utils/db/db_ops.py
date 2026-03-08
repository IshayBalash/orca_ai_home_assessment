import duckdb
import pandas as pd


class DBops:
    """DuckDB session wrapper. Abstracts all database operations"""

    def __init__(self, db_path: str):
        """
        Open the DuckDB database at db_path.
        The raw.ship_positions table is already pre-populated.
        """
        self.conn = duckdb.connect(db_path)

    def execute_query_file(self, file_path: str, params: dict = None) -> list:
        """Read a SQL file and execute it with named params (e.g. $start_ts), returning results as a list of tuples."""
        with open(file_path, "r") as f:
            sql = f.read()
        return self.conn.execute(sql, params or {}).fetchall()

    def execute_query(self, sql: str, params: dict = None) -> list:
        """Execute an inline SQL string with named params (e.g. $start_ts), returning results as a list of tuples."""
        return self.conn.execute(sql, params or {}).fetchall()

    def write_df_to_table(self, df: pd.DataFrame, table: str) -> None:
        """Write a pandas DataFrame into table (e.g. 'silver.ship_positions'). Table must already exist."""
        self.conn.execute(f"INSERT INTO {table} SELECT * FROM df")

    def delete_records(self, schema: str, table_name: str, condition: str = "1=1") -> None:
        """Delete records from schema.table_name matching condition. Defaults to all rows."""
        self.conn.execute(f"DELETE FROM {schema}.{table_name} WHERE {condition}")

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()


