import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime, date, time, timedelta
from multiprocessing import Pool
import os
fields = ['Date[L]', 'Time[L]', '#RIC', 'Type', 'Price', 'Volume', 'Bid Price', 'Bid Size', 'Ask Price', 'Ask Size',
          'Qualifiers']

appended_effective = defaultdict(float)
appended_effective_val = defaultdict(float)
appended_effective_bps = defaultdict(float)
NBBO_appended_effective = defaultdict(float)
NBBO_appended_effective_bps = defaultdict(float)
NBBO_appended_effective_vol = defaultdict(float)
NBBO_appended_effective_val = defaultdict(float)
appended_realised = defaultdict(float)
appended_realised_val = defaultdict(float)
appended_realised_bps = defaultdict(float)
NBBO_appended_realised = defaultdict(float)
NBBO_appended_realised_bps = defaultdict(float)
NBBO_appended_realised_vol = defaultdict(float)
NBBO_appended_realised_val = defaultdict(float)
appended_value_share = defaultdict(float)
appended_volume_share = defaultdict(float)
appended_BTT = []
appended_STT = []
appended_NBB_depth_share = defaultdict(float)
appended_NBO_depth_share = defaultdict(float)
appended_NBBO_depth_share = defaultdict(float)
appended_NBB_time_share = defaultdict(float)
appended_NBO_time_share = defaultdict(float)

def parse_simple_date(date_str):
    return datetime.strptime(date_str, '%Y%m%d').date()


def parse_simple_time(time_str):
    return datetime.strptime(time_str, '%H:%M:%S.%f').time()


def fast_date_parse(dates):
    return [parse_simple_date(d) for d in dates]


def parse_simple_timestamp(timestamp_str):
    return datetime.strptime(timestamp_str, '%Y%m%d%H:%M:%S.%f')


def fast_timestamp_parse(timestamps):
    return [parse_simple_timestamp(ts) for ts in timestamps]


# function to convert bid and ask size into common units as trade volume
def convert_size(df):
    df['Ask Size abs'] = np.where(df['Ask Price'] > 1, df['Ask Size'] * 100, df['Ask Size'] * 500)
    df['Bid Size abs'] = np.where(df['Bid Price'] > 1, df['Bid Size'] * 100, df['Bid Size'] * 500)
    return df

# function to fill in quoting status of the previous quote
def fill_previous_quote_status(df):
    quotes_cols = ['Bid Price', 'Bid Size', 'Ask Price', 'Ask Size', 'Ask Size abs', 'Bid Size abs']
    df[quotes_cols] = df.groupby(['venue'])[quotes_cols].fillna(method='ffill')
    return df

# function to calculate mid-quote
def calculate_mid_quote(df):
    df['MidQuote'] = np.where((df['Bid Price'] != 0) & (df['Ask Price'] != 0),
                              (df['Bid Price'] + df['Ask Price']) / 2.0, np.nan)
    return df

# function to assign trade direction
def assign_trade_direction(df):
    df['direction'] = ''
    df.loc[df['Price'] <= df['Bid Price'], 'direction'] = 'S'
    df.loc[df['Price'] >= df['Ask Price'], 'direction'] = 'B'
    df.loc[df['Price'] == df['MidQuote'], 'direction'] = 'C'
    return df

# function to create a column used to filter df rows
def create_v_col(df):
    df['v_col'] = str()
    df['v_col'] = df['v_col'] + df.groupby(['venue', 'timestamp']).cumcount().astype(str).replace('0', '')
    df['v_col'] = pd.to_numeric(df['v_col'])
    return df

# function to shift and create new columns for bid/ask price/depth
def shift_bid_ask_columns(df):
    df['Ask next'] = df.groupby('venue')['Ask Price'].shift(-1)
    df['Bid next'] = df.groupby('venue')['Bid Price'].shift(-1)
    df['Ask depth next'] = df.groupby('venue')['Ask Size abs'].shift(-1)
    df['Bid depth next'] = df.groupby('venue')['Bid Size abs'].shift(-1)
    df['Ask prev'] = df.groupby('venue')['Ask Price'].shift(1)
    df['Bid prev'] = df.groupby('venue')['Bid Price'].shift(1)
    df['Ask depth prev'] = df.groupby('venue')['Ask Size abs'].shift(1)
    df['Bid depth prev'] = df.groupby('venue')['Bid Size abs'].shift(1)
    return df
# all the exchanges' different time schedule
o_time = time(9, 45, 00)
c_time = time(15, 30, 00)

o_time_df_TO = pd.Series(['09:45:00.000', 'TO'], index=['Time[L]', 'venue'])
c_time_df_TO = pd.Series(['15:30:00.000', 'TO'], index=['Time[L]', 'venue'])
o_time_df_GO = pd.Series(['09:45:00.000', 'GO'], index=['Time[L]', 'venue'])
c_time_df_GO = pd.Series(['15:30:00.000', 'GO'], index=['Time[L]', 'venue'])
o_time_df_ALP = pd.Series(['09:45:00.000', 'ALP'], index=['Time[L]', 'venue'])
c_time_df_ALP = pd.Series(['15:30:00.000', 'ALP'], index=['Time[L]', 'venue'])
o_time_df_CXC = pd.Series(['09:45:00.000', 'CXC'], index=['Time[L]', 'venue'])
c_time_df_CXC = pd.Series(['15:30:00.000', 'CXC'], index=['Time[L]', 'venue'])
def exchange_times(df):
    df = df.append([o_time_df_TO, c_time_df_TO, o_time_df_GO, c_time_df_GO, o_time_df_ALP, c_time_df_ALP, o_time_df_CXC,
                    c_time_df_CXC], ignore_index=True)
    df[['Date[L]', 'stock']] = df[['Date[L]', 'stock']].fillna(method='ffill')
    df[['Date[L]', 'stock']] = df[['Date[L]', 'stock']].fillna(method='backfill')
    return df

