import uuid

from pandas import DataFrame


class Utils:

    @staticmethod
    def convert_int_to_uuid_version_4(value: int):
        return uuid.UUID(int=value, version=4)

    @staticmethod
    def convert_none_values(df: DataFrame):
        for col in df.columns:
            df[col] = df[col].apply(lambda x: None if x == 'None' else x)
        return df
