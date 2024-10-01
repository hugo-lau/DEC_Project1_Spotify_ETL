from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:
    """
    A client for querying postgresql database.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port
        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def create_table(self, metadata: MetaData) -> None:
        """
        Creates table provided in the metadata object
        """
        metadata.create_all(self.engine)

    