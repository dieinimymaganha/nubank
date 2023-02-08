# Importing necessary libraries

import numpy as np
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Importing custom ConnectionPostgres class for database connection
from connections.connection_postgresql import ConnectionPostgres

# Disabling the SettingWithCopyWarning
pd.options.mode.chained_assignment = None

# Establishing connection to the PostgreSQL database
connection = ConnectionPostgres.connect(
    os.getenv('DATABASE'))
# Query to retrieve the necessary data from the database
query = """select * from  investments i 
left join  d_time dt on dt.time_id = i.investment_completed_at 
where i.status = 'completed' order by dt.action_timestamp;"""

# Reading data from the database into a pandas DataFrame
df = pd.read_sql(query,
                 con=connection.engine)

# Getting the unique account IDs present in the DataFrame
account_ids = df['account_id'].unique()
# Looping through the unique account_id values
for account_id in account_ids:
    # Creating a copy of the DataFrame for the current account_id

    df_temp = df[df['account_id'] == account_id].copy()
    # Selecting the relevant columns for processing

    df_temp = df_temp[
        ['account_id', 'type', 'amount', 'status', 'action_timestamp']]
    # Converting the action_timestamp column to a datetime type and extracting the date

    df_temp['action_timestamp'] = pd.to_datetime(df_temp['action_timestamp'])
    df_temp['action_timestamp'] = df_temp['action_timestamp'].dt.date

    # Grouping the data by account_id, action_timestamp, and type, summing the amount column, and reshaping the data into a pivot table
    df_temp = df_temp.groupby(['account_id', 'action_timestamp', 'type'])[
        'amount'].sum().reset_index().pivot_table(
        index=['account_id', 'action_timestamp'], columns='type',
        values='amount').reset_index()

    # Replace missing values with 0
    df_temp.replace(np.nan, 0, inplace=True)
    # Rename columns to make code more readable
    df_temp.rename(columns={
        'investment_transfer_in': 'deposit',
        'investment_transfer_out': 'withdrawal'}, inplace=True)
    # Check for missing columns and add them with value 0 if not present
    if 'withdrawal' not in df_temp.columns:
        df_temp['withdrawal'] = 0
    if 'deposit' not in df_temp.columns:
        df_temp['deposit'] = 0
    # Sort the dataframe by action_timestamp
    df_temp.sort_values(by=['action_timestamp'], inplace=True)
    # Add a new column called 'balance' to the dataframe with NaN values
    df_temp['balance'] = np.nan
    # Assign the first value of the 'balance' column as the first deposit value multiplied by 1 + 0.0001 * 1
    df_temp['balance'][0] = df_temp['deposit'][0] * (1 + 0.0001 * 1)

    # loop through each record in df_temp
    for i in range(1, len(df_temp)):
        # retrieve the previous balance
        balance = df_temp['balance'][i - 1]
        # retrieve the deposit amount

        deposit = df_temp['deposit'][i]
        # retrieve the withdrawal amount

        withdrawal = df_temp['withdrawal'][i]
        # calculate the number of days between transactions

        days = (df_temp['action_timestamp'][i] - df_temp['action_timestamp'][
            i - 1]).days
        # calculate the current balance based on previous balance, deposit, withdrawal, and number of days

        calculate = balance * (1 + 0.0001 * days) + deposit - withdrawal
        # if the calculated balance is negative, set the current balance to the previous balance plus deposit

        if calculate < 0:
            df_temp['balance'][i] = df_temp['balance'][i - 1] * (
                    1 + 0.0001 * days) + deposit
        else:
            df_temp['balance'][i] = calculate
    # check if df_result exists in the local scope
    if 'df_result' not in locals():
        # if not, assign df_temp to df_result

        df_result = df_temp
    else:
        # if it exists, concatenate df_temp with df_result

        df_result = pd.concat([df_result, df_temp])

# Save result dataframe to a csv file
# The resulting file will be named as "investments.csv" and will be stored in the same directory as this script
# The "index" parameter is set to False to not include the index column in the csv file
df_result.to_csv('investments.csv', index=False)
