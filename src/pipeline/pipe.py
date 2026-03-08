import logging
import sys
from pathlib import Path
import yaml

from elt_utils.db.db_ops import DBops
from elt_utils.transform.dqa import Dqa
from elt_utils.transform.delta import Delta
from elt_utils.transform.publish import Publish



# Project root = orca_ai_home_assessment/
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))



logger = logging.getLogger(__name__)




_config_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_config_path, "r") as f:
    config = yaml.safe_load(f)

# Resolve relative paths in config against project root
SOURCE_DB = str(PROJECT_ROOT / config["source"]["db_path"])
TARGET_DB = str(PROJECT_ROOT / config["target"]["db_path"])


def run_pipe(start_ts: str, end_ts: str):
    """
    Entry point for the pipeline. Orchestrates all stages in order:
    init → delta → dqa → bl → publish
    """


    ### extract the batch based in the time window from source db
    logger.info(f"########Running pipeline for time window: {start_ts} to {end_ts}##########\n\n")

    logger.info(f"Connecting to source and target databases")
    source_db_manager = DBops(SOURCE_DB)
    targhet_db_manager = DBops(TARGET_DB)

    
    logger.info(f"Extracting data from source and targer databases")
    init_batch=Delta.extract_batch(source_db_manager, start_ts, end_ts)
    if init_batch.empty:
        logger.info(f"No data found in the specified time window: {start_ts} to {end_ts}")
        return
    max_ships_ts=Delta.get_max_ts_per_ship(targhet_db_manager)
    
    logger.info(f"Cleanng batch data")
    clean_batch=Dqa.dqa(batch_df=init_batch, max_ts_per_ship=max_ships_ts)
    if clean_batch.empty:
        logger.info(f"No valid data after DQA for the specified time window: {start_ts} to {end_ts}")
        return


    logger.info("post processed batch")
    post_process_df=Publish.batch_post_process(clean_batch, targhet_db_manager)


    logger.info("publishing batch")
    Publish.publish(post_process_df, targhet_db_manager, "silver.ship_positions")


    logger.info("closing database connections")
    source_db_manager.close()
    targhet_db_manager.close()

    logger.info(f"########Done Running pipeline for time window: {start_ts} to {end_ts}##########\n\n")






  
    


# def test_pipe():
#      db = DBops("data/silver/ship_positions.db")
#      print(db.execute_query("select * from silver.ship_positions"))
