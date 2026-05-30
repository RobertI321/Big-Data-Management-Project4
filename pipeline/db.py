# Postgres Connection helper

import psycopg
import os

def get_connection():
    return psycopg.connect(
        host = os.environ.get("POSTGRES_HOST", "postgres"),
        dbname = os.environ["POSTGRES_DB"],
        user = os.environ["POSTGRES_USER"],
        password = os.environ["POSTGRES_PASSWORD"]
    )
