import uuid
from pandas import DataFrame


class Utils:
    """
    Class with static methods to perform general utilitary operations.
    """

    @staticmethod
    def convert_int_to_uuid_version_4(value: int):
        """
        Converts an integer to a version 4 UUID.

        Parameters:
            value (int): The integer to be converted.

        Returns:
            uuid: A version 4 UUID generated from the input integer.
        """
        return uuid.UUID(int=value, version=4)

    @staticmethod
    def convert_none_values(df: DataFrame):
        """
        Replaces 'None' values in the dataframe with None.

        Parameters:
            df (DataFrame): The dataframe to be processed.

        Returns:
            DataFrame: The processed dataframe with the 'None' values converted to None.
        """
        for col in df.columns:
            df[col] = df[col].apply(lambda x: None if x == 'None' else x)
        return df
