import pandas as pd
import urllib.request
import os


class SetDataReader:
    """
    Getting data from SET website
    """

    def __init__(self):
        """
        :param:
        """
        self.set_ticker_master = None
        self.set_delisted_ticker_master = None

    def get_original_metadata(self):
        """
        Getting stock master table from SET
        """
        # DOWNLOADING TICKER LIST
        url = 'https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls'
        path_to_save = 'set_ticker.xls'
        urllib.request.urlretrieve(url, path_to_save)

        # STORING DATA IN VARIABLE
        self.set_ticker_master = pd.read_html(path_to_save, encoding=None)[0]

        # REMOVING DOWNLOADED FILE
        os.remove(path_to_save)

        return self.set_ticker_master

    def get_original_delisted_data(self):
        """
        Getting delisted stock master table from SET
        """
        # DOWNLOADING TICKER LIST
        url = 'https://www.set.or.th/dat/eod/listedcompany/static/delistedSecurities_en_US.xls'
        path_to_save = 'set_delisted_ticker.xls'
        urllib.request.urlretrieve(url, path_to_save)

        # STORING DATA IN VARIABLE
        self.set_delisted_ticker_master = pd.read_html(path_to_save, encoding=None)[0]

        # REMOVING DOWNLOADED FILE
        os.remove(path_to_save)

        return self.set_delisted_ticker_master

    def _transform_original_set_data(self, dataframe, local_save_path=None):
        """
        Transforming SET master table into usable form
        :param
            local_save_path: local path to save the file. If it is None, the file will not save in local storage
        """
        # SPECIFYING COLUMN NAMES
        dataframe.columns = dataframe.iloc[1]

        # REMOVING IRRELEVANT ROWS
        dataframe = dataframe.iloc[2:]

        # CREATING ACTUAL SYMBOL IN YAHOO
        dataframe['yahoo_symbol'] = dataframe['Symbol'] + '.BK'
        dataframe.reset_index(inplace=True)
        del dataframe.columns.name

        # SAVING METADATA IN LOCAL DISK
        if local_save_path is not None:
            dataframe.to_csv(local_save_path, sep='|', encoding='utf8', index=False)

        # SAVING METADATA ON GOOGLE CLOUD

        return dataframe


