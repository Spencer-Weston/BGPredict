from datetime import datetime
import pandas as pd
import numpy as np
import os

import concurrent.futures
import pickle

from pmdarima.arima import auto_arima
import sqlalchemy
from sqlalchemy import create_engine

from helpers import S3Connection
from dotenv import load_dotenv


def clean_data(df):
    # Drop rows with no Y value
    df = df.dropna(subset='bg')

    # Sort by timestamp
    df = df.sort_values(by="timestamp_clean")

    # Set index to time_stamp_clean
    df.index = df['timestamp_clean']
    df = df.drop(labels=['timestamp_clean'], axis=1)

    # Drop first row by subject which has data quality issues
    df = df[df.groupby('subjectid').cumcount() > 0]

    # Drop columns that are indices, irrelevant, or capture in OHE variables
    subject_drop_cols = list(df.columns[df.columns.str.contains('ohe')])
    drop_cols = ['timestamp', 'date', 'time']
    df = df.drop(labels=drop_cols + subject_drop_cols, axis=1)

    # Fill nulls (lag BG values) with 0 to indicate data is unavailable
    # print(f"Null values to be filled by column:")
    df = df.fillna(0)

    # One hot Encode Weekdays
    weekdays = np.unique(df['weekday'])
    ohe_weekdays = [f"ohe_{day}" for day in weekdays]
    df[ohe_weekdays] = pd.get_dummies(df.weekday)
    df = df.drop(labels="weekday", axis=1)

    return df


def load_data_by_subject(subjectid):
    location = f"postgresql://postgres:{os.environ.get('db_password')}@{os.environ.get('db_location')}"
    engine = create_engine(location)
    with engine.connect() as conn:
        raw_df = pd.read_sql(f"select * from public.vw_final_dataset where subjectid = {subjectid}", conn)
    return raw_df


def arima(subject_id):
    s3_conn = S3Connection()
    s3_folder = f"models/arima/{subject_id}"
    if s3_conn.key_exists(f"{s3_folder}/results"):
        print(f"Model for subject {subject_id} already exists. Skipping")
        return
    print(f"Loading {subject_id} data")
    raw_df = load_data_by_subject(subject_id)
    clean_df = clean_data(raw_df)

    train_df = clean_df.loc[(clean_df['train_set'] == 1) | (clean_df['validation_set'] == 1), :]
    train_y = np.array(train_df['bg'])
    test_df = clean_df.loc[clean_df['test_set'] == 1, :]
    test_y = np.array(test_df['bg'])
    drop_cols = ['subjectid', 'entryid', 'train_set', 'validation_set', 'test_set', 'observations', 'insulin_datacount',
                 'carbs_datacount', 'normalized_carbs_datapercentile', 'normalized_insulin_datapercentile', 'bg']

    train_X = np.array(train_df.drop(drop_cols, axis=1))
    test_X = np.array(test_df.drop(drop_cols, axis=1))

    print(f"Training {len(train_df)} rows on subject {subject_id} at {datetime.now()}")
    start = datetime.now()
    model_ARIMAX = auto_arima(train_y, xreg=train_X, trace=False, error_action="ignore", suppress_warnings=True)
    model_ARIMAX.fit(train_y, xreg=train_X)
    forecast_ARIMAX = model_ARIMAX.predict(n_periods=len(test_X), xreg=test_X)

    result_df = pd.DataFrame({'observed': test_y, 'predicted': forecast_ARIMAX}).reset_index(drop=True)
    join_df = clean_df.loc[clean_df['test_set'] == 1, ['subjectid', 'entryid']].reset_index(drop=True)
    result_by_entry = pd.concat([join_df, result_df], axis=1, ignore_index=True)

    result_text = result_by_entry.to_csv(index=False)
    pickled_model = pickle.dumps(forecast_ARIMAX)
    bucket = s3_conn.bucket_name
    s3_conn.s3_client.put_object(Bucket=bucket, Key=f"{s3_folder}/model", Body=pickled_model)
    s3_conn.s3_client.put_object(Bucket=bucket, Key=f"{s3_folder}/results", Body=result_text)
    print(f"Finished training {subject_id} in {datetime.now() - start}")


if __name__ == "__main__":
    print("Determining Subject ID's")
    os.chdir("../")
    load_dotenv()
    location = f"postgresql://postgres:{os.environ.get('db_password')}@{os.environ.get('db_location')}"
    engine = create_engine(location)
    with engine.connect() as conn:
        subjects = pd.read_sql(f"select DISTINCT(subjectid) from public.vw_final_dataset ", conn)
    subjects = list(subjects['subjectid'])

    with concurrent.futures.ProcessPoolExecutor() as executor:
        pool = executor.map(arima, subjects[:2])
