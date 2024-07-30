from superset.db_engine_specs import BaseEngineSpec
import re
from typing import Any
import logging

from superset.models.core import Database

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class WherobotsEngineSpec(BaseEngineSpec):
    engine = 'wherobots'
    engine_name = 'wherobots'
    supports_dynamic_schema = True
    supports_catalog = True

    @classmethod
    def adjust_engine_params(cls, uri, connect_args, schema=None, catalog=None):
        logger.info(
            f"uses WherobotsEngineSpec - adjust_engine_params() - running...")
        if schema:
            uri = uri.set(database=schema)
            connect_args['schema'] = schema
        if catalog:
            connect_args['catalog'] = catalog
        else:
            connect_args['catalog'] = 'wherobots_open_data'  # Default catalog
        return uri, connect_args

    @classmethod
    def get_catalog_names(cls, database, schema=None):
        logger.info(f"uses WherobotsEngineSpec - get_catalog_names() - running...")
        return ["wherobots", "wherobots_open_data", "wherobots_pro_data"]

    @classmethod
    def get_default_catalog(cls):
        logger.info(f"uses WherobotsEngineSpec - get_default_catalog() - running...")
        return "wherobots_open_data"

    @classmethod
    def get_all_catalogs(cls, database):
        logger.info(f"uses WherobotsEngineSpec - get_all_catalogs() - running...")
        return ["wherobots", "wherobots_open_data", "wherobots_pro_data"]

    @classmethod
    def execute(
        cls,
        cursor: Any,
        query: str,
        database: Database,
        **kwargs: Any,
    ) -> None:
        logger.info(f"uses WherobotsEngineSpec - execute() - running...")
        """
        Override the execute method to modify queries before execution.
        """
        # Add custom logic to modify the query here
        # For example, replace single quotes with double quotes in the query
        query = cls._modify_statement(query)
        query = cls._sanitize_query(query)
        # Remove GROUP BY geojson if present
        if "GROUP BY geojson" in query:
            logger.info(f"uses WherobotsEngineSpec - execute() - modifying query - {query}")
            query = query.replace("GROUP BY geojson", "")
            logger.info(f"uses WherobotsEngineSpec - execute() - modified query - {query}")

        # Call the original execute method with the modified query
        super(WherobotsEngineSpec, cls).execute(cursor, query, database, **kwargs)

    @staticmethod
    def _sanitize_query(query):
        logger.info(f"uses WherobotsEngineSpec - _sanitize_query() - running...")
        # Replace problematic alias
        query = re.sub(r"AS 'COUNT\(\*\)'", "AS count_rows", query)
        query = re.sub(r'AS "COUNT\(\*\)"', "AS count_rows", query)
        query = re.sub(r'AS "COUNT\(primary_category\)"', "AS count_primary_category", query)
        return query

    @staticmethod
    def _modify_statement(statement):
        logger.info(f"uses WherobotsEngineSpec - _modify_statement() - running...")
        logger.info(f"uses WherobotsEngineSpec - _modify_statement() - statement - {statement}")

        # Pattern to match FROM, JOIN, and IN with schema.table or schema
        pattern = re.compile(r'(?P<keyword>FROM|JOIN|IN)\s+(?P<name>\w+(\.\w+)?)', re.IGNORECASE)

        # Find all matches for the pattern
        matches = pattern.findall(statement)
        for match in matches:
            keyword, name, _ = match
            if not name.startswith('wherobots_open_data.'):
                if '.' in name:
                    # If name is in schema.table format
                    modified_name = f'wherobots_open_data.{name}'
                else:
                    # If name is just a schema
                    modified_name = f'wherobots_open_data.{name}'
                statement = statement.replace(f'{keyword} {name}', f'{keyword} {modified_name}')

        logger.info(f"uses WherobotsEngineSpec - _modify_statement() - return statement - {statement}")
        return statement
