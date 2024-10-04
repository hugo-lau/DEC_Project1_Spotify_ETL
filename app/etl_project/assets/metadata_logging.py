from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime
from sqlalchemy import Table,Column,Integer,String,MetaData, JSON
from sqlalchemy import insert, select, func

class MetaDataLoggingStatus:
    RUN_START = "start"
    RUN_SUCCESS = "success"
    RUN_FAIL = "fail"


class MetaDataLogging:
    def __init__(
            self,
            pipeline_name: str,
            postgresql_client: PostgreSqlClient,
            config: dict = {},
            log_table_name: str = "pipeline_logs",
    ):
        self.pipeline_name = pipeline_name
        self.log_table_name = log_table_name
        self.postgresql_client = postgresql_client
        self.config = config
        self.metadata = MetaData()
        self.table = Table(
            self.log_table_name,
            self.metadata,
            Column("pipeline_name", String, primary_key=True),
            Column("run_id", Integer, primary_key=True),
            Column("timestamp", String, primary_key=True),
            Column("status", String, primary_key=True),
            Column("config", JSON),
            Column("logs", String)
        )
        self.run_id: int = self._get_run_id()
    
    def _create_log_table(self) -> None:
        self.postgresql_client.create_table(metadata=self.metadata)
    

    def _get_run_id(self):
        self._create_log_table()
        run_id = self.postgresql_client.engine.execute(
            select(func.max(self.table.c.run_id)).where(
                self.table.c.pipeline_name == self.pipeline_name
            )
        ).first()[0]
        if run_id is None:
            return 1
        else:
            return run_id + 1
    
    def log(
            self,
            status: MetaDataLoggingStatus = MetaDataLoggingStatus.RUN_START,
            timestamp: datetime = None,
            logs: str = None,
    ) -> None:
        if timestamp is None:
            timestamp= datetime.now()
        insert_statement = insert(self.table).values(
            pipeline_name = self.pipeline_name,
            timestamp=timestamp,
            run_id=self.run_id,
            status=status,
            config=self.config,
            logs=logs,
        )
        self.postgresql_client.engine.execute(insert_statement)