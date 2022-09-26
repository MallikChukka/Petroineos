"""
Module that contains the  trades  aggregation functionality. This module will generate aggregated report.
"""
import datetime
import os
import logging
import sys

import pandas as pd
import trading
from datetime import date


def fetch_trades(extract_date):
    """
    Fetch trades from trades api
    :param extract_date: date to extract trades
    :return:
    """
    try:
        logging.info(f"started fetching trades for {extract_date}")
        data = trading.get_trades(extract_date)[0]
        logging.info(f"completed fetching trades for {extract_date}")
        return data
    except Exception as exception:
        raise Exception(f"failed to extract trades {exception}")


def create_dataframe_from_dict(data):
    """
    Create dataframe from dictionary
    :param data: dictionary data
    :return: Dataframe
    """
    try:
        logging.info("creating dataframe from dictionary")
        return pd.DataFrame(data).dropna()
    except Exception as exception:
        logging.error("failed to create dataframe")
        raise Exception(f"failed to create dataframe {exception}")


def derive_local_time_column(df):
    """
    derive local time from time column
    :param df: input df
    :return:
    """
    try:
        df['Local Time'] = df['time'].apply(lambda x: datetime.datetime.strptime(str(x), '%H:%M').replace(minute=0)) \
            .dt.strftime('%H:%M')
        return df
    except Exception as exception:
        raise Exception(f"failed to create local time column {exception}")


def aggregated_data(processing_date):
    """
    Aggregate hourly
    :param processing_date: date to process
    :return:
    """
    try:
        extract_data = fetch_trades(str(processing_date))
        extract_df = create_dataframe_from_dict(extract_data)
        aggregated_df = derive_local_time_column(extract_df)
        return aggregated_df.groupby('Local Time')['volume'].sum().reset_index(name='Volume')
    except Exception as ex:
        logging.info(ex)
        raise ex


def load_to_file(aggregated_df, dir_path: str):
    """
    Load dataframe to csv file.
    :param aggregated_df: aggregated dataframe
    :param dir_path: csv directory path
    :return:
    """
    try:
        logging.info(f"started loading to csv file for {dir_path}")
        if dir_path is not None:
            aggregated_df.to_csv(os.path.join(dir_path,
                                              f'PowerPosition_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'),
                                 index=False)
        else:
            aggregated_df.to_csv(f'PowerPosition_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv', index=False)
        logging.info("completed loading to csv")
    except Exception as exception:
        logging.error("failed to load to csv file")
        raise Exception(f"failed to load csv {exception}")


def get_trades_aggregated_data():
    try:
        today = date.today()
        process_date = today.strftime("%d/%m/%Y")
        current_day_aggregated_df = aggregated_data(process_date)
        previous_date = today - datetime.timedelta(days=1)
        previous_day_aggregated_df = aggregated_data(previous_date.strftime("%d/%m/%Y"))
        formatted_df = pd.concat([previous_day_aggregated_df[-1:], current_day_aggregated_df[:-1]])
        csv_directory = None
        if len(sys.argv) > 1:
            csv_directory = sys.argv[0]
        load_to_file(formatted_df, csv_directory)
    except Exception as e:
        logging.info(f"failed to process power trades {e}")


if __name__ == '__main__':
    get_trades_aggregated_data()
