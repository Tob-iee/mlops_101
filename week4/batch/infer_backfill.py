import infer_pipeline
from datetime import datetime
from dateutil.relativedelta import relativedelta

from prefect import flow


@flow
def duration_pred_pipeline_backfill():
    start_date = datetime(year=2021, month=3, day=1)
    end_date = datetime(year=2022, month=4, day=1)

    d = start_date

    while d <= end_date:
        infer_pipeline.duration_pred_pipeline(
            taxi_type='green',
            run_id='batch/model.bin',
            run_date=d
        )

        d = d + relativedelta(months=1)


if __name__ == '__main__':
   duration_pred_pipeline_backfill()