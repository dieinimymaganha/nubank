from ingestion_data.ingestion_datasets_postgresql import \
    ProcessDataSetsPostgresql

ProcessDataSetsPostgresql.run_all_ingestions(
    connection='postgresql+psycopg2://maganha:m0du10gp@localhost:5432/nubank')