import logging
import pandas as pd

logger = logging.getLogger(__name__)


class Dqa:
    """
    Responsible for handling data quality issues in the delta records.
    Each check is isolated in its own method for clarity and logging.
    """

    @staticmethod
    def _drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows where any critical field is NULL."""
        critical_cols = ["lat", "lon", "sog", "cog"]
        before = len(df)
        df = df.dropna(subset=critical_cols)
        dropped = before - len(df)
        if dropped:
            logger.info(f"drop_nulls: removed {dropped} rows with NULL in {critical_cols}")
        return df

    @staticmethod
    def _drop_invalid_coordinates(df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows where lat/lon are outside valid geographic bounds."""
        before = len(df)
        df = df[(df["lat"] >= -90) & (df["lat"] <= 90) &
                (df["lon"] >= -180) & (df["lon"] <= 180)]
        dropped = before - len(df)
        if dropped:
            logger.debug(f"drop_invalid_coordinates: removed {dropped} rows with out-of-range lat/lon")
        return df

    @staticmethod
    def _drop_invalid_sog(df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows where SOG is negative."""
        before = len(df)
        df = df[df["sog"] >= 0]
        dropped = before - len(df)
        if dropped:
            logger.debug(f"drop_invalid_sog: removed {dropped} rows with negative SOG")
        return df

    @staticmethod
    def _drop_invalid_cog(df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows where COG is outside 0–360°."""
        before = len(df)
        df = df[(df["cog"] >= 0) & (df["cog"] <= 360)]
        dropped = before - len(df)
        if dropped:
            logger.debug(f"drop_invalid_cog: removed {dropped} rows with out-of-range COG")
        return df

    @staticmethod
    def _filter_already_processed(df: pd.DataFrame, max_ts_per_ship: dict) -> pd.DataFrame:
        """
        Remove rows where source time <= max_ts already in target, per ship.
        Ships not present in the target (new ships) are fully kept.
        """
        before = len(df)
        df["_max_ts"] = df["ship_name"].map(max_ts_per_ship)

        # Normalize both sides to tz-naive for comparison
        time_naive = pd.to_datetime(df["time"]).dt.tz_localize(None)
        max_ts_naive = pd.to_datetime(df["_max_ts"]).dt.tz_localize(None)

        df = df[df["_max_ts"].isna() | (time_naive > max_ts_naive)]
        df = df.drop(columns=["_max_ts"])

        dropped = before - len(df)
        if dropped:
            logger.info(f"filter_already_processed: removed {dropped} rows already processed in target")
        return df

    @staticmethod
    def dqa(batch_df: pd.DataFrame, max_ts_per_ship: dict) -> pd.DataFrame:
        """Run all quality checks in sequence and return the cleaned DataFrame."""
        before = len(batch_df)
        df = Dqa._drop_nulls(batch_df)
        df = Dqa._drop_invalid_coordinates(df)
        df = Dqa._drop_invalid_sog(df)
        df = Dqa._drop_invalid_cog(df)
        df = Dqa._filter_already_processed(df, max_ts_per_ship)
        logger.debug(f"DQA complete — removed {before - len(df)} rows total | remaining: {len(df)}")
        return df
