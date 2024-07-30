import logging
import re
from typing import Any, List

from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.url import URL

from superset.db_engine_specs import BaseEngineSpec
from superset.models.core import Database

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class WherobotsEngineSpec(BaseEngineSpec):
    engine = "wherobots"
    engine_name = "wherobots"
    supports_dynamic_schema = True
    supports_catalog = True

    @classmethod
    def adjust_engine_params(  # pylint: disable=unused-argument
        cls,
        uri: URL,
        connect_args: dict[str, Any],
        catalog: str | None = None,
        schema: str | None = None,
    ) -> tuple[URL, dict[str, Any]]:
        logger.info("uses WherobotsEngineSpec - adjust_engine_params() - running...")
        if schema:
            uri = uri.set(database=schema)
            connect_args["schema"] = schema
        if catalog:
            connect_args["catalog"] = catalog
        else:
            connect_args["catalog"] = "wherobots_open_data"  # Default catalog
        return uri, connect_args

    @classmethod
    def get_catalog_names(
        cls,
        database: Database,
        inspector: Inspector,
    ) -> set[str]:
        logger.info("uses WherobotsEngineSpec - get_catalog_names() - running...")
        return {"wherobots", "wherobots_open_data", "wherobots_pro_data"}

    @classmethod
    def get_default_catalog(
        cls,
        database: Database,  # pylint: disable=unused-argument
    ) -> str | None:
        logger.info("uses WherobotsEngineSpec - get_default_catalog() - running...")
        return "wherobots_open_data"

    @classmethod
    def get_all_catalogs(cls) -> List[str]:
        logger.info("uses WherobotsEngineSpec - get_all_catalogs() - running...")
        return ["wherobots", "wherobots_open_data", "wherobots_pro_data"]

    @classmethod
    def execute(
        cls,
        cursor: Any,
        query: str,
        database: Database,
        **kwargs: Any,
    ) -> None:
        logger.info("uses WherobotsEngineSpec - execute() - running...")
        query = cls._append_catalog_name(query)
        query = cls._replace_alias_quotes(query)
        # Remove GROUP BY geojson if present
        if "GROUP BY geojson" in query:
            logger.info(
                "uses WherobotsEngineSpec - execute() - modifying query - %s",
                query,
            )
            query = query.replace("GROUP BY geojson", "")
            logger.info(
                "uses WherobotsEngineSpec - execute() - modified query - %s",
                query,
            )

        # Call the original execute method with the modified query
        super(WherobotsEngineSpec, cls).execute(cursor, query, database, **kwargs)

    @staticmethod
    def _replace_alias_quotes(query: str) -> str:
        """
        Replace alias names wrapped in double quotes with backticks.
        """
        logger.info("uses WherobotsEngineSpec - _replace_alias_quotes() - running...")
        logger.debug("Original query: %s", query)
        query = re.sub(r'AS\s+"([^"]+)"', r"AS `\1`", query)
        logger.debug("Modified query: %s", query)
        return query

    @staticmethod
    def _append_catalog_name(statement: str) -> str:
        logger.info("uses WherobotsEngineSpec - _append_catalog_name() - running...")
        logger.info(
            "uses WherobotsEngineSpec - _append_catalog_name() - statement - %s",
            statement,
        )

        # Pattern to match FROM, JOIN, and IN followed by a schema.table or schema
        pattern = re.compile(
            r"(?P<keyword>FROM|JOIN|IN)\s+(?P<name>\w+(\.\w+)?)", re.IGNORECASE
        )

        # Replace matched patterns with the prefixed schema name
        modified_statement = re.sub(
            pattern,
            lambda m: f'{m.group("keyword")} wherobots_open_data.{m.group("name")}',
            statement,
        )

        logger.info(
            "uses WherobotsEngineSpec - _append_catalog_name() - return statement - %s",
            modified_statement,
        )
        return modified_statement

    @classmethod
    def epoch_to_dttm(cls) -> str:
        raise NotImplementedError()
