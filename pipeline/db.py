"""Database connection helper for the RICO pipeline.

Reads connection parameters from environment variables so the same code works
inside Airflow containers (POSTGRES_HOST=postgres) and from local scripts
(POSTGRES_HOST=localhost or unset).
"""

import os
import logging

import psycopg

log = logging.getLogger(__name__)


def get_connection() -> psycopg.Connection:
    """Open and return a new psycopg connection to the rico database.

    Connection parameters are read from environment variables:
        POSTGRES_HOST  — defaults to 'localhost'
        POSTGRES_USER  — required
        POSTGRES_PASSWORD — required
        POSTGRES_DB    — defaults to 'rico'

    The caller is responsible for closing the connection (use as a
    context manager: ``with get_connection() as conn: ...``).

    Returns:
        A live psycopg.Connection in autocommit=False mode.

    Raises:
        psycopg.OperationalError: if the database is unreachable.
    """
    host = os.environ.get("POSTGRES_HOST", "localhost")
    user = os.environ.get("POSTGRES_USER", "rico")
    password = os.environ.get("POSTGRES_PASSWORD", "rico")
    dbname = os.environ.get("POSTGRES_DB", "rico")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))

    conninfo = f"host={host} port={port} dbname={dbname} user={user} password={password}"
    log.debug("Connecting to Postgres at %s:%s/%s as %s", host, port, dbname, user)

    return psycopg.connect(conninfo)
