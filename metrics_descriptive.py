def value_volume_metrics(df, filename):
    value = df.groupby(['Date[L]', 'stock', 'venue'])['Value'].sum()
    volume = df.groupby(['Date[L]', 'stock', 'venue'])['Volume'].sum()

    value.to_csv(f'.../{filename}_value.csv')
    volume.to_csv(f'...{filename}_volume.csv')
def pivot_table(df):
    df_table = df.pivot_table(
        index=['Date[L]', 'stock', 'Time[L]', 'timestamp', 'v_col', 'extra_timestamp', 'direction', 'issue',
               'vol_issue', 'TS_5min', 'extra_TS_5min'],
        columns='venue').copy()
    # df_table.to_csv('E:/Sean/OPR/new_data/tester_out/df_table_1.csv')
    df_table.loc[:,
    ['Bid Price', 'Ask Price', 'Ask Size', 'Bid Size', 'Ask Size abs', 'Bid Size abs', 'MidQuote', 'Ask next',
     'Bid next', 'Ask depth next', 'Bid depth next', 'Ask prev', 'Bid prev', 'Ask depth prev', 'Bid depth prev']] = \
        df_table[
            ['Bid Price', 'Ask Price', 'Ask Size', 'Bid Size', 'Ask Size abs', 'Bid Size abs', 'MidQuote', 'Ask next',
             'Bid next', 'Ask depth next', 'Bid depth next', 'Ask prev', 'Bid prev', 'Ask depth prev',
             'Bid depth prev']].fillna(
            method='ffill')

    # df_table.to_csv('E:/Sean/OPR/new_data/tester_out/df_table.csv')
    df_table['col'] = str()
    df_table['col'] = df_table['col'] + df_table.groupby(['timestamp']).cumcount().astype(str).replace('0', '')
    df_table['col'] = pd.to_numeric(df_table['col'])
    df_table.to_csv(f'E:/Sean/OPR/new_data/tester_out/df_table_{filename}.csv')
    # df_table.to_csv('E:/Sean/OPR/new_data/tester_out/df_table.csv')
    df_table = df_table.reset_index()
    ask_price_venue = df_table['Ask Price'].columns.astype(str).tolist()
    bid_price_venue = df_table['Bid Price'].columns.astype(str).tolist()
    mid_quote_venues = df_table['MidQuote'].columns.astype(str).tolist()
    price_venues = df_table['Price'].columns.astype(str).tolist()

    bid_cols = ['Bid Price', 'Bid Size', 'Bid Size abs']
    pivot_bid_cols = [(x, venue) for x in bid_cols for venue in bid_price_venue]
    ask_cols = ['Ask Price', 'Ask Size', 'Ask Size abs']
    pivot_ask_cols = [(x, venue) for x in ask_cols for venue in ask_price_venue]
    mid_cols = ['MidQuote']
    pivot_mid_cols = [(x, venue) for x in mid_cols for venue in mid_quote_venues]
    for v in ask_price_venue:
        df_table[('Ask Price', v)] = df_table[('Ask Price', v)].replace(
            0.0, np.NaN)
    for v in bid_price_venue:
        df_table[('Bid Price', v)] = df_table[('Bid Price', v)].replace(
            0.0, np.NaN)
    df_table['NBB'] = df_table['Bid Price'][bid_price_venue].max(axis=1)
    df_table['NBO'] = df_table['Ask Price'][ask_price_venue].min(axis=1)

    timestamp_cols = ['Date[L]', 'stock', 'timestamp', 'v_col', 'extra_timestamp']
    bid_timestamp_cols = create_quotes_table(df)[1]
    ask_timestamp_cols = create_quotes_table(df)[2]
    df_quotes = create_quotes_table(df)[0]
    timestamp_cols.extend(bid_timestamp_cols)
    timestamp_cols.extend(ask_timestamp_cols)
    df_table = df_table.merge(df_quotes[timestamp_cols],
                              on=['Date[L]', 'stock', 'timestamp', 'v_col', 'extra_timestamp'], how='left')
    for v in price_venues:
        df_table[f'bid timestamp {v} dummy'] = (df_table[f'bid timestamp {v}'].notnull()).astype(int)
        df_table[f'ask timestamp {v} dummy'] = (df_table[f'ask timestamp {v}'].notnull()).astype(int)
        # df_table_round[f'bid timestamp {v} '] = df_table_round[f'bid timestamp {v} '].fillna(method = 'ffill')
        # df_table_round[f'ask timestamp {v} '] = df_table_round[f'ask timestamp {v} '].fillna(method='ffill')
        df_table[f'bid alive {v}'] = (df_table[f'bid timestamp {v}'] - df_table.groupby(f'bid timestamp {v} dummy')[
            f'bid timestamp {v}'].shift(1)).dt.total_seconds() * 1000
        df_table[f'ask alive {v}'] = (df_table[f'ask timestamp {v}'] - df_table.groupby(f'ask timestamp {v} dummy')[
            f'ask timestamp {v}'].shift(1)).dt.total_seconds() * 1000
        df_table[f'bid alive {v}'] = df_table[f'bid alive {v}'].fillna(method='backfill')
        df_table[f'ask alive {v}'] = df_table[f'ask alive {v}'].fillna(method='backfill')
    df_table = df_table[(df_table['Time[L]'] > o_time) & (df_table['Time[L]'] < c_time)]

    return df_table, bid_price_venue, ask_price_venue, mid_quote_venues, price_venues

############################ NBBO depth time share engine #####################################3

def get_nbbo_depth_time_share_metrics(df_table, bid_price_venue, ask_price_venue,filename):
    bid_ask_venue = list(set(ask_price_venue) & set(bid_price_venue))
    NBB_venue_list = []
    NBO_venue_list = []
    NBBO_venue_list = []
    df_table['NBB_depth'] = np.nan
    df_table['NBO_depth'] = np.nan
    df_table['NBBO_depth'] = np.nan
    for v in bid_price_venue:
        df_table[f'NBB depth_{v}'] = \
            df_table[df_table['Bid Price'][v] == df_table['NBB']]['Bid Size'][v]
        NBB_venue_list.append(f'NBB depth_{v}')
    for v in ask_price_venue:
        df_table[f'NBO depth_{v}'] = \
            df_table[df_table['Ask Price'][v] == df_table['NBO']]['Ask Size'][v]
        NBO_venue_list.append(f'NBO depth_{v}')
    for v in bid_ask_venue:
        df_table[f'NBBO depth_{v}'] = df_table[f'NBO depth_{v}'] + df_table[
            f'NBB depth_{v}']
        NBBO_venue_list.append(f'NBBO depth_{v}')

    df_table['NBO_depth'] = df_table[NBO_venue_list].sum(axis=1)
    df_table['NBB_depth'] = df_table[NBB_venue_list].sum(axis=1)
    df_table['NBBO_depth'] = df_table[NBBO_venue_list].sum(axis=1)

    df_table['quote_alive'] = (df_table['timestamp'].shift(-1) - df_table[
        'timestamp']).dt.total_seconds() * 1000
    df_table['quote_alive'] = df_table['quote_alive'].replace(0, np.nan)
    df_table['quote_alive'] = df_table['quote_alive'].fillna(method='ffill')
    quotes = df_table.set_index(['stock', 'timestamp', 'col'], inplace=False)
    # quotes.to_csv('E:/Sean/OPR/new_data/tester_out/quotes.csv')
    # step 2: when venue BO = NBO, take depth of BO/NBO
    for venue in ask_price_venue:
        quotes['NBO_depth_share'] = ''
        quotes['NBO_indicator'] = ''
        quotes['NBO_depth_share'] = np.where(quotes['Ask Price'][venue] == quotes['NBO'],
                                             quotes['Ask Size'][venue].div(quotes['NBO_depth'].values), np.nan)
        quotes['NBO_depth_share_TW'] = quotes['NBO_depth_share'] * quotes['quote_alive']
        quotes['NBO_depth_share_TW'] = quotes['NBO_depth_share_TW'].replace([np.inf, -np.inf], np.nan)
        NBO_depth_share = quotes['NBO_depth_share_TW'].sum() / quotes[quotes['NBB_depth_share_TW'].notnull()][
            'quote_alive'].sum()
        quotes['NBO_indicator'] = np.where(quotes['Ask Price'][venue] == quotes['NBO'], 1, 0)
        quotes['NBO_time_alive'] = quotes['NBO_indicator'] * quotes['quote_alive']
        NBO_time_share = quotes['NBO_time_alive'].sum() / quotes['quote_alive'].sum()
        appended_NBO_depth_share[f'{filename}_{venue}'] = NBO_depth_share
        appended_NBO_time_share[f'{filename}_{venue}'] = NBO_time_share

    for venue in bid_price_venue:
        quotes['NBB_depth_share'] = ''
        quotes['NBB_indicator'] = ''
        quotes['NBB_depth_share'] = np.where(quotes['Bid Price'][venue] == quotes['NBB'],
                                             quotes['Bid Size'][venue].div(quotes['NBB_depth'].values), np.nan)
        quotes['NBB_depth_share_TW'] = quotes['NBB_depth_share'] * quotes['quote_alive']
        quotes['NBB_depth_share_TW'] = quotes['NBB_depth_share_TW'].replace([np.inf, -np.inf], np.nan)
        NBB_depth_share = quotes['NBB_depth_share_TW'].sum() / quotes[quotes['NBB_depth_share_TW'].notnull()][
            'quote_alive'].sum()

        quotes['NBB_indicator'] = np.where(quotes['Bid Price'][venue] == quotes['NBB'], 1, 0)
        quotes['NBB_time_alive'] = quotes['NBB_indicator'] * quotes['quote_alive']
        NBB_time_share = quotes['NBB_time_alive'].sum() / quotes['quote_alive'].sum()
        appended_NBB_depth_share[f'{filename}_{venue}'] = NBB_depth_share
        appended_NBB_time_share[f'{filename}_{venue}'] = NBB_time_share

    for venue in bid_ask_venue:
        quotes['NBBO_depth_share'] = ''
        quotes['NBBO_depth_share'] = np.where(
            (quotes['Ask Price'][venue] == quotes['NBO']) | (quotes['Bid Price'][venue] == quotes['NBB']),
            (quotes['Bid Size'][venue] + quotes['Ask Size'][venue]).div(quotes['NBBO_depth'].values), np.nan)

        # step3: time weight intra-day
        #     quotes = quotes.reset_index()
        quotes['NBBO_depth_share_TW'] = quotes['NBBO_depth_share'] * quotes['quote_alive']
        quotes['NBBO_depth_share_TW'] = quotes['NBBO_depth_share_TW'].replace([np.inf, -np.inf], np.nan)
        NBBO_depth_share = quotes['NBBO_depth_share_TW'].sum() / quotes[quotes['NBBO_depth_share_TW'].notnull()][
            'quote_alive'].sum()
        appended_NBBO_depth_share[f'{filename}_{venue}'] = NBBO_depth_share
    #quotes.to_csv(f'.../depth/depth_{filename}')
    appended_NBB_depth_share = pd.DataFrame.from_dict(appended_NBB_depth_share, orient='index')
    appended_NBO_depth_share = pd.DataFrame.from_dict(appended_NBO_depth_share, orient='index')
    appended_NBBO_depth_share = pd.DataFrame.from_dict(appended_NBBO_depth_share, orient='index')
    appended_NBB_time_share = pd.DataFrame.from_dict(appended_NBB_time_share, orient='index')
    appended_NBO_time_share = pd.DataFrame.from_dict(appended_NBO_time_share, orient='index')

    appended_NBB_depth_share.to_csv(
        f'.../NBB depth share/{filename}_NBB_depth_share.csv')
    appended_NBO_depth_share.to_csv(
        f'.../NBO depth share/{filename}_NBO_depth_share.csv')
    appended_NBBO_depth_share.to_csv(
        f'.../NBBO depth share/{filename}_NBBO_depth_share.csv')
    appended_NBB_time_share.to_csv(f'.../NBB time share/{filename}_NBB_time_share.csv')
    appended_NBO_time_share.to_csv(f'.../NBO time share/{filename}_NBO_time_share.csv')