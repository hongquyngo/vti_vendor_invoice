# utils/db.py

import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging
from .config import DB_CONFIG

logger = logging.getLogger(__name__)


def get_db_engine():
    """Create and return SQLAlchemy database engine"""
    logger.info("🔌 Connecting to database...")

    user = DB_CONFIG["user"]
    password = quote_plus(str(DB_CONFIG["password"]))
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    database = DB_CONFIG["database"]

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    logger.info(f"🔐 Using SQLAlchemy URL: mysql+pymysql://{user}:***@{host}:{port}/{database}")

    return create_engine(url)