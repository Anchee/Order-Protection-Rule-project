def get_quoted_spread(df_table,mid_quote_venues,filename):
    df_table['NBBO QuotedSpread'] = np.where(
        (df_table[('NBB', '')] != 0) & (df_table[('NBO', '')] != 0) & (df_table[('NBO', '')] > df_table[('NBB', '')]),
        df_table[('NBO', '')] - df_table[('NBB', '')], np.nan)

    # calculate time weight for quoted spread
    df_table['NBBO QuotedSpread_TW'] = df_table['NBBO QuotedSpread'] * df_table[('quote_alive', '')]
    df_table_quoted = df_table.copy()
    df_table_quoted.columns = [' '.join(col).strip() for col in df_table_quoted.columns.values]
    NBBO_Quoted_spread_TW = df_table_quoted.groupby(['stock', 'Date[L]'])['NBBO QuotedSpread_TW'].sum() / \
                            df_table_quoted.groupby(['stock', 'Date[L]'])[
                                'quote_alive'].sum()
    quoted = NBBO_Quoted_spread_TW.rename(f'{filename}')
    quoted.to_csv(f'.../quoted spread/{filename}_quoted.csv')




    df_table['average MidQuote'] = df_table['MidQuote'][mid_quote_venues].mean(axis=1)
    df_table[f'at tick'] = np.where(
        ((df_table['NBBO QuotedSpread'] < 0.005) & (df_table['average MidQuote'] < 2)) | (
                (df_table['NBBO QuotedSpread'] < 0.01) & (df_table['average MidQuote'] > 2)), 1, 0)
    df_table[f'at tick time'] = df_table['at tick'] * df_table[('quote_alive', '')]
    df_table_min_tick = df_table.copy()
    df_table_min_tick.columns = [' '.join(col).strip() for col in df_table_min_tick.columns.values]

    min_tick_TW = df_table_min_tick.groupby(['stock', 'Date[L]'])['at tick time'].sum() / \
                  df_table_min_tick.groupby(['stock', 'Date[L]'])[
                      'quote_alive'].sum()

    tick_time = min_tick_TW.rename(f'{filename}')
    tick_time.to_csv(f'.../tick time/{filename}_tick time.csv')


    Quoted_spread_dict = defaultdict()

    for v in mid_quote_venues:
        df_table[f'QuotedSpread {v}'] = np.where(
            (df_table[('Bid Price', f'{v}')] != 0) & (df_table[('Ask Price', f'{v}')] != 0) & (
                    df_table[('Ask Price', f'{v}')] > df_table[('Bid Price', f'{v}')]),
            df_table[('Ask Price', f'{v}')] - df_table[('Bid Price', f'{v}')], np.nan)

        # calculate time weight for quoted spread
        df_table[f'QuotedSpread_TW {v}'] = df_table['NBBO QuotedSpread'] * df_table[f'bid alive {v}']
        df_table_quoted = df_table.copy()
        df_table_quoted.columns = [' '.join(col).strip() for col in df_table_quoted.columns.values]
        # df_table_quoted.to_csv(f'E:/Sean/OPR/new_data/tester_out/df_table_quoted {filename}')
        Quoted_spread_TW = df_table_quoted[f'QuotedSpread_TW {v}'].sum() / \
                           df_table_quoted[
                               f'bid alive {v}'].sum()
        Quoted_spread_dict[f'{filename}_{v}'] = Quoted_spread_TW
    appended_quoted_spread = pd.DataFrame.from_dict(Quoted_spread_dict, orient='index')
    appended_quoted_spread.to_csv(
        f'.../per venue quoted spread/per venue quote spread {filename}')

def get_effective_spread(df_table, filename, price_venues, mid_quote_venues):

    df_table['NBBO_MidQuote'] = np.where(
    (df_table[('NBB', '')] != 0) & (df_table[('NBO', '')] != 0) & (df_table[('NBO', '')] > df_table[('NBB', '')]),
    (df_table[('NBO', '')] + df_table[('NBB', '')]) / 2, np.nan)



    df_table_ES = df_table[
    ['Date[L]', 'timestamp', 'col', 'stock', 'Price', 'Value', 'Volume', 'direction', 'NBBO_MidQuote','Bid Price','Ask Price','Bid Size','Ask Size','MidQuote']]

    df_table_ES.columns = [' '.join(col).strip() for col in df_table_ES.columns.values]


    price_venues = list(filter(None, price_venues))

    for venue in mid_quote_venues:

        b_sel = df_table_ES.direction == 'B'
        df_table_ES.loc[b_sel, f'{venue} NBBO Effective Spread'] = 2 * (df_table_ES.loc[b_sel, f'Price {venue}'] -
                                                                            df_table_ES.loc[b_sel, 'NBBO_MidQuote'])
        s_sel = df_table_ES.direction == 'S'
        df_table_ES.loc[s_sel, f'{venue} NBBO Effective Spread'] = 2 * (df_table_ES.loc[s_sel, 'NBBO_MidQuote'] -
                                                                            df_table_ES.loc[s_sel, f'Price {venue}'])

        NBBO_Effective_Spread = df_table_ES[f'{venue} NBBO Effective Spread'].mean()
        df_table_ES[f'{venue} NBBO Effective Spread_VW'] = df_table_ES[f'{venue} NBBO Effective Spread'] * df_table_ES[
            f'Value {venue}']
        NBBO_Value_weighted_Effective_Spread = np.where(df_table_ES[f'Value {venue}'].sum() != 0,df_table_ES[f'{venue} NBBO Effective Spread_VW'].sum() /
        df_table_ES[f'Value {venue}'].sum(), np.nan)

        df_table_ES[f'{venue} NBBO Effective Spread_VoW'] = df_table_ES[f'{venue} NBBO Effective Spread'] * df_table_ES[
            f'Volume {venue}']
        NBBO_Volume_weighted_Effective_Spread = np.where(df_table_ES[f'Volume {venue}'].sum() != 0,
                                                        df_table_ES[f'{venue} NBBO Effective Spread_VoW'].sum() /
                                                        df_table_ES[f'Volume {venue}'].sum(), np.nan)

        b_sel = df_table_ES.direction == 'B'
        df_table_ES.loc[b_sel, f'{venue} NBBO Effective Spread bps'] = 2 * (df_table_ES.loc[b_sel, f'Price {venue}'] -
                                                                        df_table_ES.loc[b_sel, 'NBBO_MidQuote'])/df_table_ES.loc[b_sel, 'NBBO_MidQuote']
        s_sel = df_table_ES.direction == 'S'
        df_table_ES.loc[s_sel, f'{venue} NBBO Effective Spread bps'] = 2 * (df_table_ES.loc[s_sel, 'NBBO_MidQuote'] -
                                                                        df_table_ES.loc[s_sel, f'Price {venue}'])/df_table_ES.loc[s_sel, 'NBBO_MidQuote']

        NBBO_Effective_Spread_bps = df_table_ES[f'{venue} NBBO Effective Spread bps'].mean()

        NBBO_appended_effective[f'{filename}_{venue}'] = NBBO_Effective_Spread
        NBBO_appended_effective_val[f'{filename}_{venue}'] = NBBO_Value_weighted_Effective_Spread
        NBBO_appended_effective_vol[f'{filename}_{venue}'] = NBBO_Volume_weighted_Effective_Spread
        NBBO_appended_effective_bps[f'{filename}_{venue}'] = NBBO_Effective_Spread_bps


    NBBO_appended_effective = pd.DataFrame.from_dict(NBBO_appended_effective, orient='index')
    NBBO_appended_effective_val = pd.DataFrame.from_dict(NBBO_appended_effective_val, orient='index')
    NBBO_appended_effective_bps = pd.DataFrame.from_dict(NBBO_appended_effective_bps, orient='index')
    NBBO_appended_effective_vol = pd.DataFrame.from_dict(NBBO_appended_effective_vol, orient='index')
    NBBO_appended_effective.to_csv(
            f'.../NBBO effective spread/{filename}_effective.csv')
    NBBO_appended_effective_bps.to_csv(
            f'.../NBBO effective spread bps/{filename}_effective_bps.csv')
    NBBO_appended_effective_val.to_csv(
            f'.../NBBO effective spread value weighted/{filename}_effective_val_weighted.csv')
    NBBO_appended_effective_vol.to_csv(
            f'.../NBBO effective spread volume weighted/{filename}_effective_vol_weighted.csv')
    venues = list(set(price_venues) & set(mid_quote_venues))
    for venue in venues:
        df_table_ES.loc[b_sel, f'{venue} Effective Spread'] = 2 * (df_table_ES.loc[b_sel, f'Price {venue}'] -
                                                                            df_table_ES.loc[b_sel, f'MidQuote {venue}'])
        df_table_ES.loc[s_sel, f'{venue} Effective Spread'] = 2 * (df_table_ES.loc[s_sel, f'MidQuote {venue}'] -
                                                                            df_table_ES.loc[s_sel, f'Price {venue}'])
        Effective_Spread = df_table_ES[f'{venue} Effective Spread'].mean()
        df_table_ES.loc[b_sel, f'{venue} Effective Spread bps'] = 2 * (df_table_ES.loc[b_sel, f'Price {venue}'] -
                                                                            df_table_ES.loc[b_sel, f'MidQuote {venue}'])/df_table_ES.loc[b_sel, f'MidQuote {venue}']

        df_table_ES.loc[s_sel, f'{venue} Effective Spread bps'] = 2 * (df_table_ES.loc[s_sel, f'MidQuote {venue}'] -
                                                                            df_table_ES.loc[s_sel, f'Price {venue}'])/df_table_ES.loc[s_sel, f'MidQuote {venue}']

        Effective_Spread_bps = df_table_ES[f'{venue} Effective Spread bps'].mean()


        df_table_ES[f'{venue} Effective Spread_VW'] = df_table_ES[f'{venue} Effective Spread'] * df_table_ES[
                f'Value {venue}']


        Value_weighted_Effective_Spread = np.where(df_table_ES[f'Value {venue}'].sum()!= 0,
                    df_table_ES[f'{venue} Effective Spread_VW'].sum() /
                    df_table_ES[f'Value {venue}'].sum(),np.nan)

        appended_effective[f'{filename}_{venue}'] = Effective_Spread
        appended_effective_val[f'{filename}_{venue}'] = Value_weighted_Effective_Spread
        appended_effective_bps[f'{filename}_{venue}'] = Effective_Spread_bps
    appended_effective = pd.DataFrame.from_dict(appended_effective, orient='index')
    appended_effective_bps = pd.DataFrame.from_dict(appended_effective_bps, orient='index')
    appended_effective_val = pd.DataFrame.from_dict(appended_effective_val, orient='index')
    appended_effective.to_csv(f'.../effective spread/{filename}_effective.csv')
    appended_effective_bps.to_csv(
            f'.../effective spread bps/{filename}_effective_bps.csv')
    appended_effective_val.to_csv(
            f'.../effective spread value weighted/{filename}_effective_val_weighted.csv')

def get_realised_spread(df_table, ask_price_venue, bid_price_venue, price_venues,filename):

    df_table_nbbo_RS = pd.merge_asof(df_table,
                                     df_table,
                                     left_on='extra_TS_5min', right_on='extra_timestamp',
                                     by='stock', suffixes=('', '_5min'),
                                     allow_exact_matches=False)
    df_table_nbbo_RS.columns = [' '.join(col).strip() for col in df_table_nbbo_RS.columns.values]
    # df_table_nbbo_RS.to_csv('E:/Sean/OPR/new_data/tester_out/df_table_nbbo_rs.csv')
    ask_price_venue_NBBO_5min = list()
    bid_price_venue_NBBO_5min = list()
    for v in ask_price_venue:
        df_table_nbbo_RS[f'Ask Price_5min {v}'] = df_table_nbbo_RS[f'Ask Price_5min {v}'].fillna(method='ffill')
        df_table_nbbo_RS[f'Ask Price_5min {v}'] = df_table_nbbo_RS[f'Ask Price_5min {v}'].replace(
                0.0, np.NaN)
        ask_price_venue_NBBO_5min.append(f'Ask Price_5min {v}')
    for v in bid_price_venue:
        df_table_nbbo_RS[f'Bid Price_5min {v}'] = df_table_nbbo_RS[f'Bid Price_5min {v}'].fillna(method='ffill')
        df_table_nbbo_RS[f'Bid Price_5min {v}'] = df_table_nbbo_RS[f'Bid Price_5min {v}'].replace(
                0.0, np.NaN)
        bid_price_venue_NBBO_5min.append(f'Bid Price_5min {v}')
    df_table_nbbo_RS['NBB_5min'] = df_table_nbbo_RS[bid_price_venue_NBBO_5min].max(axis=1)
    df_table_nbbo_RS['NBO_5min'] = df_table_nbbo_RS[ask_price_venue_NBBO_5min].min(axis=1)
    try:
        df_table_nbbo_RS['NBB_5min'] = np.where((df_table_nbbo_RS['NBB_5min'] >= df_table_nbbo_RS['NBO_5min']),
                                                    df_table_nbbo_RS['Bid Price_5min TO'], df_table_nbbo_RS['NBB_5min'])
        df_table_nbbo_RS['NBO_5min'] = np.where((df_table_nbbo_RS['NBB_5min'] >= df_table_nbbo_RS['NBO_5min']),
                                                    df_table_nbbo_RS['Ask Price_5min TO'], df_table_nbbo_RS['NBO_5min'])

    # df_table_nbbo_RS.to_csv('E:/Sean/OPR/new_data/tester_out/df_table_nbbo_rs_1.csv')
    df_table_nbbo_RS['NBBO_MidQuote_5min'] = np.where(
            (df_table_nbbo_RS['NBB_5min'] != 0) & (df_table_nbbo_RS['NBO_5min'] != 0) & (
                df_table_nbbo_RS['NBO_5min'] > df_table_nbbo_RS['NBB_5min']),
            (df_table_nbbo_RS['NBO_5min'] + df_table_nbbo_RS['NBB_5min']) / 2, np.nan)

    for venue in price_venues:

        df_table_nbbo_RS[f'NBBO Realized Spread {venue}'] = np.nan
        b_sel = df_table_nbbo_RS.direction == 'B'
        df_table_nbbo_RS.loc[b_sel, f'NBBO Realized Spread {venue}'] = 2 * (
        df_table_nbbo_RS.loc[b_sel, f'Price {venue}'] -
        df_table_nbbo_RS.loc[b_sel, f'NBBO_MidQuote_5min'])
        s_sel = df_table_nbbo_RS.direction == 'S'
        df_table_nbbo_RS.loc[s_sel, f'NBBO Realized Spread {venue}'] = 2 * (
        df_table_nbbo_RS.loc[s_sel, f'NBBO_MidQuote_5min'] -
        df_table_nbbo_RS.loc[s_sel, f'Price {venue}'])

        NBBO_realised_spread = df_table_nbbo_RS[[f'NBBO Realized Spread {venue}']].mean()
        NBBO_realised_spread_val = np.take(NBBO_realised_spread.values, 0)

        df_table_nbbo_RS[f'NBBO Realised Spread_VW {venue}'] = df_table_nbbo_RS[
                                                                       f'NBBO Realized Spread {venue}'] * \
                                                                   df_table_nbbo_RS[
                                                                       f'Value {venue}']
        # df_table_RS.to_csv(f'E:/Sean/OPR/new_data/new output 2/df_table_RS/df_table_RS_{filename}{venue}.csv')
        nbbo_sum_RSVW = np.float64(df_table_nbbo_RS[f'NBBO Realised Spread_VW {venue}'].sum())
        nbbo_sum_V = np.float64(df_table_nbbo_RS[f'Value {venue}'].sum())
        NBBO_Value_weighted_Realised_Spread = nbbo_sum_RSVW / nbbo_sum_V

        df_table_nbbo_RS[f'NBBO Realised Spread_VoW {venue}'] = df_table_nbbo_RS[
                                                                        f'NBBO Realized Spread {venue}'] * \
                                                                    df_table_nbbo_RS[
                                                                        f'Volume {venue}']
        # df_table_RS.to_csv(f'E:/Sean/OPR/new_data/new output 2/df_table_RS/df_table_RS_{filename}{venue}.csv')
        nbbo_sum_RSVoW = np.float64(df_table_nbbo_RS[f'NBBO Realised Spread_VoW {venue}'].sum())
        nbbo_sum_Vo = np.float64(df_table_nbbo_RS[f'Volume {venue}'].sum())
        NBBO_Volume_weighted_Realised_Spread = nbbo_sum_RSVoW / nbbo_sum_Vo

        df_table_nbbo_RS[f'NBBO Realized Spread bps {venue}'] = np.nan
        # b_sel = df_table_nbbo_RS.direction == 'B'
        df_table_nbbo_RS.loc[b_sel, f'NBBO Realized Spread bps {venue}'] = 2 * (
                df_table_nbbo_RS.loc[b_sel, f'Price {venue}'] -
                df_table_nbbo_RS.loc[b_sel, f'NBBO_MidQuote_5min']) / df_table_nbbo_RS.loc[
                                                                                   b_sel, f'NBBO_MidQuote_5min']
        # s_sel = df_table_nbbo_RS.direction == 'S'
        df_table_nbbo_RS.loc[s_sel, f'NBBO Realized Spread bps {venue}'] = 2 * (
                df_table_nbbo_RS.loc[s_sel, f'NBBO_MidQuote_5min'] -
                df_table_nbbo_RS.loc[s_sel, f'Price {venue}']) / df_table_nbbo_RS.loc[s_sel, f'NBBO_MidQuote_5min']

        NBBO_realised_spread_bps = df_table_nbbo_RS[[f'NBBO Realized Spread bps {venue}']].mean()
        NBBO_realised_spread_bps = np.take(NBBO_realised_spread_bps.values, 0)

        NBBO_appended_realised[f'{filename}_{venue}'] = NBBO_realised_spread_val
        NBBO_appended_realised_val[f'{filename}_{venue}'] = NBBO_Value_weighted_Realised_Spread
        NBBO_appended_realised_vol[f'{filename}_{venue}'] = NBBO_Volume_weighted_Realised_Spread
        NBBO_appended_realised_bps[f'{filename}_{venue}'] = NBBO_realised_spread_bps
            # df_table_nbbo_RS.to_csv(f'E:/Sean/OPR/new_data/tester_out/df_table_RS_{filename}')

        df_table_nbbo_RS[f'Realized Spread {venue}'] = np.nan
        b_sel = df_table_nbbo_RS.direction == 'B'
        df_table_nbbo_RS.loc[b_sel, f'Realized Spread {venue}'] = 2 * (df_table_nbbo_RS.loc[b_sel, f'Price {venue}'] -
                                                                      df_table_nbbo_RS.loc[
                                                                          b_sel, f'MidQuote_5min {venue}'])
        s_sel = df_table_nbbo_RS.direction == 'S'
        df_table_nbbo_RS.loc[s_sel, f'Realized Spread {venue}'] = 2 * (
        df_table_nbbo_RS.loc[s_sel, f'MidQuote_5min {venue}'] -
        df_table_nbbo_RS.loc[s_sel, f'Price {venue}'])
        realised_spread = df_table_nbbo_RS[[f'Realized Spread {venue}']].mean()
        df_table_nbbo_RS[f'Realized Spread bps {venue}'] = np.nan
        b_sel = df_table_nbbo_RS.direction == 'B'
        df_table_nbbo_RS.loc[b_sel, f'Realized Spread bps {venue}'] = 2 * (df_table_nbbo_RS.loc[b_sel, f'Price {venue}'] -
                                                                          df_table_nbbo_RS.loc[
                                                                              b_sel, f'MidQuote_5min {venue}']) / \
                                                                     df_table_nbbo_RS.loc[
                                                                         b_sel, f'MidQuote_5min {venue}']
        s_sel = df_table_nbbo_RS.direction == 'S'
        df_table_nbbo_RS.loc[s_sel, f'Realized Spread bps {venue}'] = 2 * (
        df_table_nbbo_RS.loc[s_sel, f'MidQuote_5min {venue}'] -
        df_table_nbbo_RS.loc[s_sel, f'Price {venue}']) / df_table_nbbo_RS.loc[s_sel, f'MidQuote_5min {venue}']

        realised_spread_bps = df_table_nbbo_RS[[f'Realized Spread bps {venue}']].mean()
        realised_spread_val = np.take(realised_spread.values, 0)
        realised_spread_bps = np.take(realised_spread_bps.values, 0)
        df_table_nbbo_RS[f'Realised Spread_VW {venue}'] = df_table_nbbo_RS[f'Realized Spread {venue}'] * df_table_nbbo_RS[
                f'Value {venue}']

        sum_RSVW = np.float64(df_table_nbbo_RS[f'Realised Spread_VW {venue}'].sum())
        sum_V = np.float64(df_table_nbbo_RS[f'Value {venue}'].sum())
        Value_weighted_Realised_Spread = sum_RSVW / sum_V
        appended_realised[f'{filename}_{venue}'] = realised_spread_val
        appended_realised_val[f'{filename}_{venue}'] = Value_weighted_Realised_Spread
        appended_realised_bps[f'{filename}_{venue}'] = realised_spread_bps


    appended_realised = pd.DataFrame.from_dict(appended_realised, orient='index')
    appended_realised_val = pd.DataFrame.from_dict(appended_realised_val, orient='index')
    appended_realised_bps = pd.DataFrame.from_dict(appended_realised_bps, orient='index')

    NBBO_appended_realised = pd.DataFrame.from_dict(NBBO_appended_realised, orient='index')
    NBBO_appended_realised_val = pd.DataFrame.from_dict(NBBO_appended_realised_val, orient='index')
    NBBO_appended_realised_bps = pd.DataFrame.from_dict(NBBO_appended_realised_bps, orient='index')
    NBBO_appended_realised_vol = pd.DataFrame.from_dict(NBBO_appended_realised_vol, orient='index')
    appended_realised.to_csv(f'.../realised spread/{filename}_realised.csv')
    appended_realised_bps.to_csv(
            f'.../realised spread bps/{filename}_realised_bps.csv')
    appended_realised_val.to_csv(
            f'.../realised spread value weighted/{filename}_realised_val_weighted.csv')
    NBBO_appended_realised.to_csv(f'.../NBBO realised spread/{filename}_realised.csv')
    NBBO_appended_realised_bps.to_csv(
        f'.../NBBO realised spread bps/{filename}_realised_bps.csv')
    NBBO_appended_realised_val.to_csv(
        f'.../NBBO realised spread value weighted/{filename}_realised_val_weighted.csv')
    NBBO_appended_realised_vol.to_csv(
        f'.../NBBO realised spread volume weighted/{filename}_realised_vol_weighted.csv')
