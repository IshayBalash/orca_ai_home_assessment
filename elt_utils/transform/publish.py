import logging
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from elt_utils.db.db_ops import DBops

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent.parent.parent / "sql"
_GET_LAST_ROW_SQL = str(_SQL_DIR / "get_last_row_per_ship.sql")

EARTH_RADIUS_NM = 3440.065


class Publish:
    """
    Responsible for enriching and writing processed delta records to the target tables.
    """

    @staticmethod
    def _get_last_row_per_ship(target_db: DBops) -> pd.DataFrame:
        """
        Fetch the last known row per ship from silver table.
        Returns empty DataFrame if silver table has no data (first run or new ship).
        """
        rows = target_db.execute_query_file(_GET_LAST_ROW_SQL)
        return pd.DataFrame(rows, columns=["ship_name", "time", "lat", "lon", "sog", "cog"])

    @staticmethod 

    ## need to validate algorithm.
    def _haversine(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
        """Vectorized Haversine distance in nautical miles."""
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        return EARTH_RADIUS_NM * 2 * np.arcsin(np.sqrt(a))

    @staticmethod
    def batch_post_process(clean_batch: pd.DataFrame, target_db: DBops) -> pd.DataFrame:
        """
        Enrich the clean batch with rot, acceleration, and distance_traveled.
        Uses the last known row per ship from silver as seed rows to correctly
        compute lag-based values for the first record of each ship in the batch.
        New ships with no prior history get NULL for enrichment fields on their first record.
        """
        seed_rows = Publish._get_last_row_per_ship(target_db)

        if not seed_rows.empty:
            seed_rows["_is_seed"] = True
            clean_batch["_is_seed"] = False
            combined = pd.concat([seed_rows, clean_batch], ignore_index=True)
        else:
            clean_batch["_is_seed"] = False
            combined = clean_batch.copy()

        # Normalize time to tz-naive UTC so sort works across mixed timezone sources
        combined["time"] = pd.to_datetime(combined["time"], utc=True).dt.tz_convert(None)
        combined = combined.sort_values(["ship_name", "time"]).reset_index(drop=True)

        # Compute lag values per ship
        grouped = combined.groupby("ship_name", sort=False)
        combined["prev_cog"] = grouped["cog"].shift(1)
        combined["prev_sog"] = grouped["sog"].shift(1)
        combined["prev_lat"] = grouped["lat"].shift(1)
        combined["prev_lon"] = grouped["lon"].shift(1)

        # Calculate enrichments — NaN where no previous row exists (first record per ship)
        combined["rot"] = ((combined["cog"] - combined["prev_cog"]) + 180) % 360 - 180
        combined["acceleration"] = combined["sog"] - combined["prev_sog"]
        combined["distance_traveled"] = Publish._haversine(
            combined["prev_lat"], combined["prev_lon"],
            combined["lat"], combined["lon"]
        )

        # Drop seed rows and helper columns
        result = combined[~combined["_is_seed"]].drop(
            columns=["_is_seed", "prev_cog", "prev_sog", "prev_lat", "prev_lon"]
        )

        # Rename time → time_ts and reorder columns to match target schema
        result = result.rename(columns={"time": "time_ts"})
        result = result[["ship_name", "time_ts", "lat", "lon", "sog", "cog",
                         "rot", "acceleration", "distance_traveled"]]

        logger.info(f"batch_post_process: enriched {len(result)} rows")
        return result.reset_index(drop=True)

    @staticmethod
    def publish(post_processed_df: pd.DataFrame, target_db: DBops, target_table: str) -> None:
        """Add elt_published_at and write enriched batch to the target table."""
        post_processed_df["elt_published_at"] = datetime.now(timezone.utc)
        target_db.write_df_to_table(post_processed_df, target_table)
        logger.info(f"publish: inserted {len(post_processed_df)} rows into {target_table}")
