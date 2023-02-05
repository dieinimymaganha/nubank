from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class ConnectionPostgres:
    """
    Class to create and manage a connection to a PostgreSQL database using SQLAlchemy.

    Attributes:
        database_uri (str): The database URI used for connection.
        engine (SQLAlchemy engine object): The SQLAlchemy engine used for connection.
        session (SQLAlchemy session object): The SQLAlchemy session used for connection.

    """

    def __init__(self):
        self.database_uri = None
        self.engine = None
        self.session = None

    @classmethod
    def connect(cls, connection):
        """
        Connects to the PostgreSQL database using SQLAlchemy.

        Args:
            connection (str): The URI to the database.

        Returns:
            connection_database (ConnectionPostgres): An instance of the class, with a connected session to the database.

        """
        connection_database = cls()
        connection_database.database_uri = connection
        connection_database.engine = create_engine(
            connection_database.database_uri, echo=False, pool_recycle=1800
        )
        Session = sessionmaker(bind=connection_database.engine)
        connection_database.session = Session()

        return connection_database
