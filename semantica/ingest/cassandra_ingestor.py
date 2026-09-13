"""
Cassandra Ingestion Module

Provides Cassandra data ingestion capabilities for the Semantica framework.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..utils.exceptions import ProcessingError
from ..utils.logging import get_logger


try:
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    CASSANDRA_AVAILABLE = True
except (ImportError, OSError):
    Cluster = None
    PlainTextAuthProvider = None
    CASSANDRA_AVAILABLE = False


@dataclass
class CassandraData:
    """Cassandra data representation."""

    data: List[Dict[str, Any]]
    row_count: int
    columns: List[str]
    keyspace: str
    table_name: str
    schema: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    ingested_at: datetime = field(default_factory=datetime.now)


class CassandraConnector:
    """Manage connections to a Cassandra cluster."""

    def __init__(
        self,
        hosts: Optional[List[str]] = None,
        port: int = 9042,
        username: Optional[str] = None,
        password: Optional[str] = None,
        keyspace: Optional[str] = None,
        **config: Any,
    ) -> None:
        if not CASSANDRA_AVAILABLE:
            raise ImportError(
                "cassandra-driver is required for CassandraConnector. "
                "Install it with: pip install 'semantica[db-cassandra]'"
            )

        self.logger = get_logger("cassandra_connector")

        self.hosts = hosts or ["127.0.0.1"]
        self.port = port
        self.username = username
        self.password = password
        self.keyspace = keyspace
        self.config = config

        self.cluster = None
        self.session = None

    def connect(self):
        """Connect to Cassandra and return the session."""
        try:
            auth_provider = None

            if self.username and self.password:
                auth_provider = PlainTextAuthProvider(
                    username=self.username,
                    password=self.password,
                )

            self.cluster = Cluster(
                contact_points=self.hosts,
                port=self.port,
                auth_provider=auth_provider,
                **self.config,
            )

            if self.keyspace:
                self.session = self.cluster.connect(self.keyspace)
            else:
                self.session = self.cluster.connect()

            self.logger.info("Connected to Cassandra")
            return self.session

        except Exception as exc:
            self.cluster = None
            self.session = None
            self.logger.error(
                "Failed to connect to Cassandra: %s",
                type(exc).__name__,
            )
            raise ProcessingError(
                f"Failed to connect to Cassandra: {type(exc).__name__}"
            ) from exc

    def disconnect(self) -> None:
        """Close the Cassandra connection."""
        if self.session is not None:
            try:
                self.session.shutdown()
            except Exception:
                pass
            finally:
                self.session = None

        if self.cluster is not None:
            try:
                self.cluster.shutdown()
            except Exception:
                pass
            finally:
                self.cluster = None

        self.logger.info("Disconnected from Cassandra")

    def test_connection(self) -> bool:
        """Test whether Cassandra is reachable."""
        try:
            session = self.connect()
            session.execute("SELECT release_version FROM system.local")
            return True
        except Exception as exc:
            self.logger.debug(
                "Cassandra connection test failed: %s",
                type(exc).__name__,
            )
            return False
        finally:
            self.disconnect()

class CassandraIngestor:
    """Cassandra data ingestion handler."""

    def __init__(
        self,
        hosts: Optional[List[str]] = None,
        port: int = 9042,
        username: Optional[str] = None,
        password: Optional[str] = None,
        keyspace: Optional[str] = None,
        connector: Optional[CassandraConnector] = None,
        **config: Any,
    ) -> None:
        self.logger = get_logger("cassandra_ingestor")

        self.connector = connector or CassandraConnector(
            hosts=hosts,
            port=port,
            username=username,
            password=password,
            keyspace=keyspace,
            **config,
        )

        self.keyspace = keyspace or self.connector.keyspace

    def ingest_table(
        self,
        table_name: str,
        keyspace: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> CassandraData:
        """Ingest rows and schema information from a Cassandra table."""

        keyspace = keyspace or self.keyspace

        if not keyspace:
            raise ValueError("keyspace is required")

        session = self.connector.connect()

        try:
            schema = self.get_table_schema(
                table_name=table_name,
                keyspace=keyspace,
            )

            columns = [column["name"] for column in schema["columns"]]

            query = f"SELECT * FROM {keyspace}.{table_name}"

            if limit is not None:
                query += f" LIMIT {int(limit)}"

            result = session.execute(query)

            rows = [dict(row._asdict()) if hasattr(row, "_asdict") else dict(row) for row in result]

            return CassandraData(
                data=rows,
                row_count=len(rows),
                columns=columns,
                keyspace=keyspace,
                table_name=table_name,
                schema=schema,
                metadata={"query": query},
            )

        except Exception as exc:
            self.logger.error(
                "Failed to ingest Cassandra table: %s",
                type(exc).__name__,
            )
            raise ProcessingError(
                f"Failed to ingest Cassandra table: {type(exc).__name__}"
            ) from exc
