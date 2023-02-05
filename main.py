from process_data.read_and_process_postgresql import \
    ProcessDataSetsPostgresql


def main(uri_connection_postgresql):
    ProcessDataSetsPostgresql.run_all_ingestions(
        connection=uri_connection_postgresql)


if __name__ == '__main__':
    main('postgresql+psycopg2://maganha:m0du10gp@localhost:5432/nubank')
