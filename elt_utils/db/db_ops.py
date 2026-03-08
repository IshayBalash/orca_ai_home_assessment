import duckdb

class DBops:
    """DuckDB session wrapper. Abstracts all database operations"""

    def __init__(self, db_path: str):
        """
        Open the DuckDB database at db_path.
        The raw.ship_positions table is already pre-populated.
        """
        self.conn = duckdb.connect(db_path)

    def execute_query_file(self, file_path: str, params: list = None) -> list:
        """Read a .sql file from disk, execute it with optional params, return all rows."""
        pass

    def write_df_to_table(self, df: "pd.DataFrame", schema: str, table_name: str) -> None:
        """Write a pandas DataFrame into sql.table_name."""

    def close(self) -> None:
        """Close the DuckDB connection."""
        pass


