# Order-Protection-Rule-project
First project in PhD
This project includes functions for data preprocessing and analysis of stock market data in relation to the implementation of the Order Protection Rule event. 

## Table of Contents
- [Preprocess Data](#preprocess-data)
- [Create Quotes Table](#create-quotes-table)
- [Exchange Times](#exchange-times)
- [Value Volume Metrics](#value-volume-metrics)
- [Pivot Table](#pivot-table)
- [NBBO Depth Time Share Engine](#nbbo-depth-time-share-engine)
- [Get NBBO Depth Time Share Metrics](#get-nbbo-depth-time-share-metrics)
- [Get Quoted Spread]
- [Get Effective Spread]
- [Get Realised Spread]

## Preprocess Data
The `preprocess_data` function performs data preprocessing tasks on the input DataFrame. It includes steps such as converting timestamps to datetime, separating dataframes by stock and exchange, filling missing values, and filtering data based on certain conditions.

## Create Quotes Table
The `create_quotes_table` function creates a quotes table from the input DataFrame. It pivots the data and organizes it by date, stock, timestamp, and venue. It also extracts bid and ask timestamps for each venue.

## Exchange Times
The `exchange_times` function adds exchange times to the input DataFrame. It appends rows containing the opening and closing times for different exchanges.

## Value Volume Metrics
The `value_volume_metrics` function calculates value and volume metrics from the input DataFrame. It groups the data by date, stock, and venue, and computes the sum of values and volumes. The results are saved as separate CSV files.

## Pivot Table
The `pivot_table` function creates a pivot table from the input DataFrame. It organizes the data by various columns, including date, stock, timestamp, direction, issue, volume issue, and time intervals. It also performs data cleaning and manipulation tasks.

## NBBO Depth Time Share Engine
This section includes functions related to the NBBO (National Best Bid and Offer) depth time share engine. It calculates metrics such as NBB (National Best Bid), NBO (National Best Offer), and NBBO (National Best Bid and Offer) depth. It also determines the quote alive time for each timestamp.

## Get NBBO Depth Time Share Metrics
The `get_nbbo_depth_time_share_metrics` function calculates NBBO depth time share metrics based on the input DataFrame. It calculates NBB depth, NBO depth, and NBBO depth for each venue. It also computes the quote alive time for each timestamp.

## Get Quoted Spread
This function calculates the quoted spread for a given DataFrame `df_table` containing financial data. The `mid_quote_venues` parameter is a list of venues to consider for the mid-quote calculation. The `filename` parameter is the name of the output file. The function calculates the time-weighted quoted spread and saves the result in a CSV file.


## Get Effective Spread
This function calculates the effective spread for a given DataFrame df_table containing financial data. The filename parameter is the name of the output file. The price_venues and mid_quote_venues parameters are lists of venues to consider for the price and mid-quote calculations, respectively. The function calculates various metrics such as value-weighted and volume-weighted effective spread and saves the results in separate CSV files.

## Get Realized Spread
This function calculates the realized spread for a given DataFrame df_table containing financial data. The ask_price_venue, bid_price_venue, and price_venues parameters are lists of venues to consider for the ask price, bid price, and price calculations, respectively. The filename parameter is the name of the output file. The function calculates the realized spread and saves the result in a CSV file.



Please refer to the code for more details on each function and their usage.




