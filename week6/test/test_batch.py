import batch
import pandas as pd
from pandas.testing import assert_frame_equal

from datetime import datetime

def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)

def test_prepare_data():

    data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
    categorical = ['PUlocationID', 'DOlocationID']
    df = pd.DataFrame(data, columns=columns)

    actual_dataframe = batch.prepare_data(df, categorical)
    actual_df_dict = actual_dataframe.to_dict(orient='records')
    actual_dict_len = len(actual_df_dict)


    expected_output = [{'PUlocationID': '-1',
    'DOlocationID': '-1',
    'pickup_datetime': dt(1, 2),
    'dropOff_datetime': dt(1, 10),
    'duration': 8.000000000000002},
    {'PUlocationID': '1',
    'DOlocationID': '1',
    'pickup_datetime': dt(1, 2),
    'dropOff_datetime': dt(1, 10),
    'duration': 8.000000000000002}
    ]
    expected_dataframe = pd.DataFrame.from_dict(expected_output)
    expected_df_dict = expected_dataframe.to_dict(orient='records')
    expected_dict_len = len(expected_df_dict)

    assert actual_dict_len == expected_dict_len
    assert_frame_equal(actual_dataframe, expected_dataframe)
