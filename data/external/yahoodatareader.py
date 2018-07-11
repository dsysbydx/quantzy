import pandas as pd
from pandas_datareader import data as pdr
import fix_yahoo_finance as yf
import sys


class YahooDataReader:
    """
    Reading data from YAHOO Finance
    """
    def __init__(self):
        """
        No initial parameters yet
        """


    def get_price_data(self, ticker_list, start_date, end_date, progress_log=True):
        """
        Getting price data from Yahoo given in ticker_list
        ticker_list: list of ticker
        start_date: start date of the data
        end_date: the last date of the data
        :return: price data
        """
        # FIXED ERROR
        yf.pdr_override()

        # DOWNLOADING DATA
        error_list = []
        data = pdr.get_data_yahoo(
                        ticker_list[0],
                        start=start_date, end=end_date,
                        progress=False
                        ).reset_index()
        data['ticker'] = ticker_list[0][:-3]
        progress_n = 1
        for ticker in ticker_list[1:]:
            if progress_log:
                sys.stdout.write("\rDownloading {0}/{1}".format(str(progress_n), str(len(ticker_list))))
                sys.stdout.flush()
            progress_n += 1
            try:
                download_df = pdr.get_data_yahoo(
                            ticker,
                            start=start_date, end=end_date,
                            progress=False
                            ).reset_index()
                download_df['ticker'] = ticker[:-3]
                data = pd.concat([data, download_df], ignore_index=True)
            except:
                error_list.append(ticker)
        if progress_log:
            print("\n")
        print(error_list)

        return data, error_list

    def get_t(self, price_dataframe, ticker_independent_t=True, date_dependent_t=True):
        """
        Getting time t for each ticker
        :param price_dataframe:
        :return:
        """
        # CREATING TICKER t
        if ticker_independent_t:
            # GETTING t FOR EACH TICKER
            price_dataframe = price_dataframe.sort_values(['ticker', 'Date'])
            price_dataframe['ticker_t'] = price_dataframe.groupby(['ticker']).cumcount()

        #
        if date_dependent_t:
            # GETTING COMMON t FOR ALL TICKER
            time_master = pd.DataFrame(df['Date'].unique()).sort_values([0])
            time_master.reset_index(drop=True, inplace=True)
            time_master.reset_index(inplace=True)
            time_master.columns = ['time_t', 'Date']
            price_dataframe = pd.merge(price_dataframe, time_master, how='left', on='Date')

        return price_dataframe
