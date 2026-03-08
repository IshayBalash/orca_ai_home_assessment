from datetime import datetime, timedelta
import logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

from pipeline.pipe import run_pipe, test_pipe



if __name__ == "__main__":
    start_date = datetime(2026, 1, 28) ## first day in the data
    end_date   = datetime(2026, 2, 22) ## last day in the data
    total_days = (end_date - start_date).days + 1

    for day in range(total_days):
        current  = start_date + timedelta(days=day)
        next_day = current + timedelta(days=1)
        run_pipe(
            start_ts=current.strftime("%Y-%m-%d %H:%M:%S"),
            end_ts=next_day.strftime("%Y-%m-%d %H:%M:%S")
        )

   