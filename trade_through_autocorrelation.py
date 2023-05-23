def generate_column_names(prefix, venues):
    return [f"{prefix} {v}" for v in venues]


def round_dataframe(df, decimals):
    return df.round(decimals)


def find_trade_throughs(df, price_venues):
    buy_trade_throughs = []
    sell_trade_throughs = []

    for venue in price_venues:
        buy_venue_got_traded_through = df.loc[
            (df['direction '] == 'B') & (df[f'Price {venue}'] > df['NBO '])
        ]

        sell_venue_got_traded_through = df.loc[
            (df['direction '] == 'S') & (df[f'Price {venue}'] < df['NBB '])
        ]

        buy_trade_throughs.append(buy_venue_got_traded_through)
        sell_trade_throughs.append(sell_venue_got_traded_through)

    return pd.concat(buy_trade_throughs), pd.concat(sell_trade_throughs)

def save_trade_throughs_to_csv(btt_df, stt_df):
    btt_df.to_csv(os.path.join(f'.../buy_trade_throughs.csv'), index=False)
    stt_df.to_csv(os.path.join(f'.../sell_trade_throughs.csv'), index=False)


def compute_trade_throughs(df_table,price_venues):
    df_table_TT = df_table.copy()
    # ask_price_venue_NBBO = generate_column_names("Ask Price", ask_price_venue)
    # bid_price_venue_NBBO = generate_column_names("Bid Price", bid_price_venue)
    # price_venue_NBBO = generate_column_names("Price", price_venues)
    # volume_venue_NBBO = generate_column_names("Volume", price_venues)
    # value_venue_NBBO = generate_column_names("Value", price_venues)

    decimals = pd.Series([3], index=['Ask Price', 'Ask Size', 'Bid Price', 'Bid Size', 'Price', 'Volume', 'NBB', 'NBO',
                                     'MidQuote', 'Value'])

    df_table_round = round_dataframe(df_table_TT, decimals)
    df_table_round.columns = [' '.join(col) for col in df_table_round.columns]

    appended_BTT, appended_STT = find_trade_throughs(df_table_round, price_venues)
    save_trade_throughs_to_csv(appended_BTT, appended_STT)

def process_autocorrelation(df, price_venues, o_time, c_time, filename):
    file_date = df['Date[L]'].values[0]
    file_year = file_date.year
    file_month = file_date.month
    file_day = file_date.day

    autocorrelation_dict = defaultdict()
    variance_dict = defaultdict()

    time_grid = pd.date_range(datetime(file_year, file_month, file_day, hour=9, minute=45),
                              datetime(file_year, file_month, file_day, hour=15, minute=30), freq='100L')

    time_grid_df = pd.DataFrame({'time': time_grid})
    df_grid_quotes_list = list()
    for v in price_venues:
        time_grid_df['venue'] = v
        per_venue_df = df.loc[df['venue'] == v]
        per_venue_df = per_venue_df[(per_venue_df['Time[L]'] > o_time) & (per_venue_df['Time[L]'] < c_time)]
        per_venue_df[['stock', 'Time[L]', 'extra_timestamp', 'MidQuote']] = per_venue_df[['stock', 'Time[L]', 'extra_timestamp', 'MidQuote']].dropna()

        df_grid_quotes = pd.merge_asof(time_grid_df, per_venue_df[['stock', 'Time[L]', 'extra_timestamp', 'MidQuote']],
                                       left_on='time', right_on='extra_timestamp', allow_exact_matches=True, direction='backward')

        df_grid_quotes['r'] = np.log(df_grid_quotes['MidQuote'].shift(-1) / df_grid_quotes['MidQuote'])
        df_grid_quotes = df_grid_quotes[df_grid_quotes['MidQuote'].notnull()]
        df_grid_quotes = df_grid_quotes[df_grid_quotes['r'].notnull()]
        midquote_autocorrelation = df_grid_quotes['r'].autocorr(lag=6)
        midquote_variance = df_grid_quotes['r'].var()
        autocorrelation_dict[f'{filename}_{venue}'] = midquote_autocorrelation
        variance_dict[f'{filename}_{venue}'] = midquote_variance
        df_grid_quotes = df_grid_quotes.rename(columns={'r': f'r {v}', 'MidQuote': f'MidQuote {v}'})
        df_grid_quotes_list.append(df_grid_quotes[['time', f'r {v}', f'MidQuote {v}']])

    midquote_autocorrelation_df = pd.DataFrame.from_dict(autocorrelation_dict, orient='index')
    midquote_variance_df = pd.DataFrame.from_dict(variance_dict, orient='index')

    midquote_autocorrelation_df.to_csv(os.path.join( f'.../{filename}_midquote_autocorrelation.csv'))
    midquote_variance_df.to_csv(os.path.join( f'.../{filename}_midquote_variance.csv'))

    df_grid_quotes_all = pd.concat(df_grid_quotes_list, axis=1, keys='time')
    correlation = df_grid_quotes_all.corr(method='pearson')
    correlation.to_csv(os.path.join('.../corr_{filename}.csv'))



