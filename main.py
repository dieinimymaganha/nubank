from process_data.read_and_process_postgresql import \
    ProcessDataSetsPostgresql
import os
from dotenv import load_dotenv

load_dotenv()


def main(uri_connection_postgresql):
    ProcessDataSetsPostgresql.run_all_ingestions(
        connection=uri_connection_postgresql)


if __name__ == '__main__':
    main(os.getenv('DATABASE'))
