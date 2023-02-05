import os

import pandas as pd
import glob

from pandas import DataFrame

from connections.connection_postgresql import ConnectionPostgres
from utils.utils import Utils
import json


class ProcessDataSetsPostgresql:
    def __init__(self, connection: str):
        self.connection = ConnectionPostgres.connect(connection)
        self.files_account = glob.glob("Tables/accounts/*.csv")
        self.files_customers = glob.glob("Tables/customers/*.csv")
        self.files_city = glob.glob("Tables/city/*.csv")
        self.files_state = glob.glob("Tables/state/*.csv")
        self.files_country = glob.glob("Tables/country/*.csv")
        self.files_d_year = glob.glob("Tables/d_year/*.csv")
        self.files_d_month = glob.glob("Tables/d_month/*.csv")
        self.files_d_week = glob.glob("Tables/d_week/*.csv")
        self.files_d_weekday = glob.glob("Tables/d_weekday/*.csv")
        self.files_d_time = glob.glob("Tables/d_time/*.csv")
        self.files_investiments = glob.glob("Tables/investments/*.txt")
        self.files_pix_moviments = glob.glob("Tables/pix_movements/*")
        self.files_transfer_ins = glob.glob("Tables/transfer_ins/*")
        self.files_transfer_out = glob.glob("Tables/transfer_outs/*")

    def read_and_process_accounts(self):
        """
        This function reads a set of account csv files and processes the data before ingestion.
        The data is processed by converting the "account_id" and "customer_id" columns from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The processed data is then ingested into a table named "accounts".

        Parameters:
        None

        Returns:
        None
        """
        for file in self.files_account:
            df = pd.read_csv(file)
            df["account_id"] = df["account_id"].apply(
                Utils.convert_int_to_uuid_version_4)
            df["customer_id"] = df["customer_id"].apply(
                Utils.convert_int_to_uuid_version_4)
            self.ingestion(df=df, table_name='accounts')

    def read_and_process_customers(self):
        """
        This function reads a set of customer csv files and processes the data before ingestion.
        The data is processed by converting the "customer_id" column from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The "customer_city" column is also converted from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The "cpf" column is converted from its original format to a string.
        The processed data is then ingested into a table named "customers".

        Parameters:
        None

        Returns:
        None
        """
        for file in self.files_customers:
            df = pd.read_csv(file)
            df["customer_id"] = df["customer_id"].apply(
                Utils.convert_int_to_uuid_version_4)
            df['customer_city'] = df['customer_city'].apply(
                Utils.convert_int_to_uuid_version_4)
            df['cpf'] = df['cpf'].apply(lambda x: str(x))
            self.ingestion(df=df, table_name='customers')

    def read_and_process_city(self):
        """
        This function reads a set of city csv files and processes the data before ingestion.
        The data is processed by converting the "city_id" column from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The "state_id" column is also converted from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The processed data is then ingested into a table named "city".

        Parameters:
        None

        :return:
        """
        for file in self.files_city:
            df = pd.read_csv(file)
            df["city_id"] = df["city_id"].apply(
                Utils.convert_int_to_uuid_version_4)
            df["state_id"] = df["state_id"].apply(
                Utils.convert_int_to_uuid_version_4)
            self.ingestion(df=df, table_name='city')

    def read_and_process_states(self):
        """
        This function reads a set of state csv files and processes the data before ingestion.
        The data is processed by concatenating all the dataframes obtained from reading each file.
        The "state_id" column is then converted from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The "country_id" column is also converted from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The processed data is then ingested into a table named "states".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_state:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        df["state_id"] = df["state_id"].apply(
            Utils.convert_int_to_uuid_version_4)
        df["country_id"] = df["country_id"].apply(
            Utils.convert_int_to_uuid_version_4)
        self.ingestion(df=df, table_name='states')

    def read_and_process_country(self):
        """
        This function reads a set of country csv files, concatenates them into a single DataFrame, and processes the data before ingestion.
        The data is processed by converting the "country_id" column from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The processed data is then ingested into a table named "country".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_country:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        df["country_id"] = df["country_id"].apply(
            Utils.convert_int_to_uuid_version_4)
        self.ingestion(df=df, table_name='country')

    def read_and_process_state(self):
        """
        This function reads a set of state csv files, concatenates them into a single DataFrame, and processes the data before ingestion.
        The data is processed by converting the "state_id" and "country_id" columns from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The processed data is then ingested into a table named "state".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_state:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        df["state_id"] = df["state_id"].apply(
            Utils.convert_int_to_uuid_version_4)
        df["country_id"] = df["country_id"].apply(
            Utils.convert_int_to_uuid_version_4)
        self.ingestion(df=df, table_name='state')

    def read_and_process_dimension_year(self):
        """
        This function reads a set of dimension year csv files and processes the data before ingestion.
        The data is processed by concatenating all the dataframes obtained from reading each file.
        The processed data is then ingested into a table named "d_year".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_d_year:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_year')

    def read_and_process_dimension_month(self):
        """
        This function reads a set of dimension month csv files and processes the data before ingestion.
        The data is processed by concatenating all the dataframes obtained from reading each file.
        The processed data is then ingested into a table named "d_month".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_d_month:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_month')

    def read_and_process_dimension_week(self):
        """
        This function reads a set of dimension week csv files and processes the data before ingestion.
        The data is processed by concatenating all the dataframes obtained from reading each file.
        The processed data is then ingested into a table named "d_week".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_d_week:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_week')

    def read_and_process_dimension_weekday(self):
        """
        This function reads a set of dimension weekday csv files and processes the data before ingestion.
        The data is processed by concatenating all the dataframes obtained from reading each file.
        The processed data is then ingested into a table named "d_weekday".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_d_weekday:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_weekday')

    def read_and_process_dimension_time(self):
        """
        This function reads a set of dimension time csv files and processes the data before ingestion.
        The data is processed by concatenating all the dataframes obtained from reading each file.
        The processed data is then ingested into a table named "d_time".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_d_time:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_time')

    def read_and_process_investiments(self):
        """
        This function reads a set of investment json files and processes the data before ingestion.
        The data is processed by concatenating all the dataframes obtained from reading and normalizing each file.
        The 'None' values are then converted using the Utils.convert_none_values function.
        The "transaction_id" column is then converted from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The "account_id" column is also converted from integers to UUID version 4 using the Utils.convert_int_to_uuid_version_4 function.
        The "investment_completed_at_timestamp" column is then dropped.
        The processed data is then ingested into a table named "investments".

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_investiments:
            with open(file) as file_read:
                data = json.load(file_read)

            df = pd.json_normalize(data, 'transactions', ['account_id'])
            list_df.append(df)
        df = pd.concat(list_df)

        df = Utils.convert_none_values(df)
        df["transaction_id"] = df["transaction_id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(int(x)))
        df["account_id"] = df["account_id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(int(x)))
        df.drop(columns=['investment_completed_at_timestamp'],
                inplace=True)

        self.ingestion(df=df, table_name='investments')

    def read_and_process_pix_movements(self):
        """
        Reads and processes data from files in `self.files_pix_moviments` into a single Pandas dataframe and ingests the data into the 'pix_movements' table.
        The function first reads data from each file in `self.files_pix_moviments` and appends the data to a list of Pandas dataframes `list_df`. The data is either read as an Excel (.xlsx) or a CSV file.
        The data from each file is then concatenated into a single Pandas dataframe `df`. The data is processed to convert `None` values to a suitable data type.
        The data in the columns 'id' and 'account_id' are converted from integers to UUID version 4 using the `Utils.convert_int_to_uuid_version_4` function. The columns 'pix_amount', 'pix_requested_at', and 'pix_completed_at' are converted from their current data type to `float`, `int`, and `int` or `None`, respectively. The columns 'status' and 'in_or_out' are converted to the `str` data type.
        The data is then sorted by all columns and duplicates are dropped, keeping the last record. The processed data is then ingested into the 'pix_movements' table using the `self.ingestion` method.

        Parameters:
        None

        Returns:
        None
         """
        list_df = []
        for file in self.files_pix_moviments:

            name, ext = os.path.splitext(file)
            if ext == '.xlsx':
                df = pd.read_excel(file)
                list_df.append(df)
            if ext == '.csv':
                df = pd.read_csv(file)
                list_df.append(df)

        df = pd.concat(list_df)
        df = Utils.convert_none_values(df)

        df["id"] = df["id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(int(x)))
        df["account_id"] = df["account_id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(int(x)))
        df['pix_amount'] = df['pix_amount'].apply(lambda x: float(x))
        df['pix_requested_at'] = df['pix_requested_at'].apply(lambda x: int(x))
        df['pix_completed_at'] = df['pix_completed_at'].apply(
            lambda x: int(x) if x is not None else None)
        df['status'] = df['status'].apply(lambda x: str(x))
        df['in_or_out'] = df['in_or_out'].apply(lambda x: str(x))
        df.sort_values(by=list(df.columns), inplace=True)
        df.drop_duplicates(subset=list(df.columns), keep='last', inplace=True)
        self.ingestion(df=df, table_name='pix_movements')

    def read_and_process_transfer_ins(self):
        """
        This function reads and processes the data from the files stored in the `self.files_transfer_ins` list.
        The files are either in .xlsx or .csv format, and the function concatenates all the data frames into one data frame.
        The data is then cleaned using the `Utils.convert_none_values` function, and then transformed using the `self.transform_data_transfer` function.
        Finally, the processed data is ingested into the 'transfer_ins' table using the `self.ingestion` function.

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_transfer_ins:
            name, ext = os.path.splitext(file)
            if ext == '.xlsx':
                df = pd.read_excel(file)
                list_df.append(df)
            if ext == '.csv':
                df = pd.read_csv(file)
                list_df.append(df)
        df = pd.concat(list_df)
        df = Utils.convert_none_values(df)
        self.transform_data_transfer(df=df)
        self.ingestion(df=df, table_name='transfer_ins')

    def read_and_process_transfer_out(self):
        """
        Reads and processes all the transfer out files stored in the `files_transfer_out` attribute of the class.
        The function first concatenates all the dataframes into a single one, then processes them using the
        `convert_none_values` method of the Utils class, then passes the resulting dataframe to the `transform_data_transfer`
        method. Finally, the resulting dataframe is ingested into the 'transfer_outs' table.

        Parameters:
        None

        Returns:
        None
        """
        list_df = []
        for file in self.files_transfer_out:
            name, ext = os.path.splitext(file)
            if ext == '.xlsx':
                df = pd.read_excel(file)
                list_df.append(df)
            if ext == '.csv':
                df = pd.read_csv(file)
                list_df.append(df)
        df = pd.concat(list_df)
        df = Utils.convert_none_values(df)
        self.transform_data_transfer(df=df)
        self.ingestion(df=df, table_name='transfer_outs')

    @staticmethod
    def transform_data_transfer(df):
        """
        Transform data of a given dataframe (df) into a specific format.

        This method receives a dataframe (df), and it applies some transformations on it.
        The transformations include converting "id" and "account_id" columns into UUID v4, converting
        "amount" column into float, converting "transaction_requested_at" and "transaction_completed_at" columns
        into float and int respectively. Finally, the method sorts the dataframe by its columns and drops duplicates.

        Args:
        df: pandas.DataFrame
            The dataframe to be transformed.

        Returns:
        pandas.DataFrame
            The transformed dataframe.
        """
        df["id"] = df["id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(int(x)))
        df["account_id"] = df["account_id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(int(x)))
        df['amount'] = df['amount'].apply(lambda x: float(x))
        df['transaction_requested_at'] = df['transaction_requested_at'].apply(
            lambda x: float(x))
        df['transaction_completed_at'] = df['transaction_completed_at'].apply(
            lambda x: int(x) if x is not None else None)
        df.sort_values(by=list(df.columns), inplace=True)
        df.drop_duplicates(subset=list(df.columns), keep='last', inplace=True)
        return df

    def ingestion(self, df: DataFrame, table_name: str):

        """
        Ingests data from a pandas DataFrame into a SQL database table.

        :param df: The pandas DataFrame to be ingested.
        :param table_name: The name of the SQL database table to be ingested into.

        Returns
        None
        """

        df.to_sql(table_name, con=self.connection.engine,
                  if_exists="append",
                  index=False)

    @classmethod
    def run_all_ingestions(cls, connection):
        run = cls(connection=connection)
        run.read_and_process_country()
        run.read_and_process_state()
        run.read_and_process_city()
        run.read_and_process_accounts()
        run.read_and_process_customers()
        run.read_and_process_dimension_year()
        run.read_and_process_dimension_month()
        run.read_and_process_dimension_week()
        run.read_and_process_dimension_weekday()
        run.read_and_process_dimension_time()
        run.read_and_process_investiments()
        run.read_and_process_pix_movements()
        run.read_and_process_transfer_ins()
        run.read_and_process_transfer_out()
