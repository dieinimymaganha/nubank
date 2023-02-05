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
                lambda x: Utils.convert_int_to_uuid_version_4(x))
            df["customer_id"] = df["customer_id"].apply(
                lambda x: Utils.convert_int_to_uuid_version_4(x))
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
                lambda x: Utils.convert_int_to_uuid_version_4(x))
            df['customer_city'] = df['customer_city'].apply(
                lambda x: Utils.convert_int_to_uuid_version_4(x))
            df['cpf'] = df['cpf'].apply(lambda x: str(x))
            self.ingestion(df=df, table_name='customers')

    def read_and_process_city(self):
        for file in self.files_city:
            df = pd.read_csv(file)
            df["city_id"] = df["city_id"].apply(
                lambda x: Utils.convert_int_to_uuid_version_4(x))
            df["state_id"] = df["state_id"].apply(
                lambda x: Utils.convert_int_to_uuid_version_4(x))
            self.ingestion(df=df, table_name='city')

    def read_and_process_state(self):
        list_df = []
        for file in self.files_state:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        df["state_id"] = df["state_id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(x))
        df["country_id"] = df["country_id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(x))
        self.ingestion(df=df, table_name='state')

    def read_and_process_country(self):
        list_df = []
        for file in self.files_country:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        df["country_id"] = df["country_id"].apply(
            lambda x: Utils.convert_int_to_uuid_version_4(x))
        self.ingestion(df=df, table_name='country')

    def read_and_process_dimension_year(self):
        list_df = []
        for file in self.files_d_year:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_year')

    def read_and_process_dimension_month(self):
        list_df = []
        for file in self.files_d_month:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_month')

    def read_and_process_dimension_week(self):
        list_df = []
        for file in self.files_d_week:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_week')

    def read_and_process_dimension_weekday(self):
        list_df = []
        for file in self.files_d_weekday:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_weekday')

    def read_and_process_dimension_time(self):
        list_df = []
        for file in self.files_d_time:
            df = pd.read_csv(file)
            list_df.append(df)

        df = pd.concat(list_df)
        self.ingestion(df=df, table_name='d_time')

    def read_and_process_investiments(self):
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
        # Durante a analise identificado que existiam 2 arquivos na
        # pasta table. Para garantir que não havia nenhum dado sendo ignorado
        # realizei a leitura dos dois formatos, ajustei todos os formatos dos
        # dados e removi dados duplicados para garantir que todas as
        # informações foram salvas.

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
