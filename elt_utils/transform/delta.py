import logging
from pathlib import Path
import pandas as pd
from elt_utils.db.db_ops import DBops

logger = logging.getLogger(__name__)

# SQL file paths — relative to project root, hardcoded here
_SQL_DIR = Path(__file__).parent.parent.parent / "sql"
_EXTRACT_BATCH_SQL = str(_SQL_DIR / "extract_new_batch.sql")
_GET_MAX_TS_SQL = str(_SQL_DIR / "get_max_ts_per_ship.sql")


class Delta:
    """
    Responsible for extracting a new batch of unprocessed source records.
    Handles both full load (new ships) and incremental load (existing ships).
    """

    @staticmethod
    def extract_batch(source_db: DBops, start_ts: str, end_ts: str) -> pd.DataFrame:
        """Extract a batch of records from the source table using the provided time window."""
        params = {"start_ts": start_ts, "end_ts": end_ts}
        rows = source_db.execute_query_file(_EXTRACT_BATCH_SQL, params)
        df = pd.DataFrame(rows, columns=["time", "ship_name", "lat", "lon", "sog", "cog"])
        logger.info(f"Extracted {len(df)} rows from source for window [{start_ts} → {end_ts}]")
        return df

    @staticmethod
    def get_max_ts_per_ship(target_db: DBops) -> dict:
        """
        Query the target table for max(time_ts) per ship.
        Returns a dict: {ship_name: max_timestamp}.
        Ships not yet in the target will not appear — treated as no lower bound.
        """
        rows = target_db.execute_query_file(_GET_MAX_TS_SQL)
        return {ship: max_ts for ship, max_ts in rows}

