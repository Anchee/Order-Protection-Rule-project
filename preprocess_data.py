def preprocess_data(df):
    # convert timestamps to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # separate dataframes by stock and exchange
    stock_exchange_groups = df.groupby(['stock', 'exchange'])
    #stock_exchange_dfs = [group.copy() for _, group in stock_exchange_groups]

    df['Date[L]'] = df['timestamp'].dt.date
    df['Time[L]'] = df['timestamp'].dt.time
    df['v_col'] = df['v_col'].fillna(0)
    df['intra_time'] = pd.to_timedelta(df['v_col'], unit='us')
    df['extra_timestamp'] = df['timestamp'] + df['intra_time']
    df['TS_5min'] = df['timestamp'] + timedelta(seconds=1)
    df['extra_TS_5min'] = df['extra_timestamp'] + timedelta(seconds=1)

    df[['Bid Size', 'Ask Size', 'Ask Size abs', 'Bid Size abs']] = df[
        ['Bid Size', 'Ask Size', 'Ask Size abs', 'Bid Size abs']].fillna(0.0)
    df['Value'] = df['Price'] * df['Volume']
    df = df.drop(df[df['Value'] > 2000000].index)

    df['issue'] = np.where(((df['direction'] == 'S') & (df['Price'] < df['Bid Price'])) | (
            (df['direction'] == 'B') & (df['Price'] > df['Ask Price'])), 1, 0)

    df['vol_issue'] = np.where(((df['direction'] == 'S') & (df['Volume'] > df['Bid Size abs'])) | (
            (df['direction'] == 'B') & (df['Volume'] > df['Ask Size abs'])), 1, 0)

    df['Qualifiers'] = df['Qualifiers'].str.replace('[PRC_QL_CD];', '')
    df = df.drop(df[df['Qualifiers'] == "242[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "2[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "32[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "32[IRGCOND]; [PRC_QL2]"].index)
    df = df.drop(df[df['Qualifiers'] == "32[IRGCOND];MOC[PRC_QL2]"].index)
    df = df.drop(df[df['Qualifiers'] == "252[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "344[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "BAS[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "BAS[IRGCOND];BAS[PRC_QL2]"].index)
    df = df.drop(df[df['Qualifiers'] == "CM[PRC_QL2]"].index)
    df = df.drop(df[df['Qualifiers'] == "CM[PRC_QL2];High[USER]"].index)
    df = df.drop(df[df['Qualifiers'] == "CM[PRC_QL2];Low[USER]"].index)
    df = df.drop(df[df['Qualifiers'] == "CM[PRC_QL2];Open|High|Low[USER]"].index)
    df = df.drop(df[df['Qualifiers'] == "CON[PRC_QL2]"].index)
    df = df.drop(df[df['Qualifiers'] == "CT[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "MS[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "NN[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "VWP[IRGCOND]"].index)
    df = df.drop(df[df['Qualifiers'] == "VWP[IRGCOND];VWP[PRC_QL2]"].index)

    return df

def create_quotes_table(df):
    # NaN values implie no depth
    df_quotes = df.pivot_table(['Bid Price', 'Ask Price', 'MidQuote', 'Time[L]', 'Bid Size', 'Ask Size'],
                               ['Date[L]', 'stock', 'timestamp', 'v_col', 'extra_timestamp'], 'venue').copy()
    df_quotes.reset_index(inplace=True)
    # keep timestamp for quotes on each venue

    ask_price_venue = df_quotes['Ask Price'].columns.astype(str).tolist()
    bid_price_venue = df_quotes['Bid Price'].columns.astype(str).tolist()
    bid_ask_venue = list(set(ask_price_venue) & set(bid_price_venue))
    ask_timestamp_cols = list()
    bid_timestamp_cols = list()

    for v in ask_price_venue:
        df_quotes[f'ask timestamp {v}'] = df_quotes['timestamp'].where(df_quotes['Ask Price'][v].notnull())
        ask_timestamp_cols.append(f'ask timestamp {v}')
    for v in bid_price_venue:
        df_quotes[f'bid timestamp {v}'] = df_quotes['timestamp'].where(df_quotes['Bid Price'][v].notnull())
        bid_timestamp_cols.append(f'bid timestamp {v}')
    return df_quotes, bid_timestamp_cols, ask_timestamp_cols

