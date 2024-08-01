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
    engine = "Wherobots"
    engine_name = "Wherobots"
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
        # else:
        #     connect_args["catalog"] = "wherobots_open_data"  # Default catalog
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

        logger.info(
            "uses WherobotsEngineSpec - execute() - modifying query - %s",
            query,
        )

        # Normalize whitespace for consistent matching
        normalized_query = " ".join(query.split())
        logger.info("Normalized query: %s", normalized_query)

        # Handle GROUP BY clauses
        modified_query = cls._remove_group_by_geojson(normalized_query)
        logger.info(
            "Modified query after removing GROUP BY on ST_AsGeoJSON aliases: %s",
            modified_query,
        )

        # Call the original execute method with the modified query
        super(WherobotsEngineSpec, cls).execute(
            cursor, modified_query, database, **kwargs
        )

    @staticmethod
    def _remove_group_by_geojson(query: str) -> str:
        # Extract aliases for ST_AsGeoJSON with any parameters in the SELECT statement
        select_clause = re.search(r"SELECT (.*) FROM", query, re.IGNORECASE)
        if select_clause:
            select_fields = select_clause.group(1)
            geojson_aliases = re.findall(
                r"ST_AsGeoJSON\([^\)]*\)(?:\s+AS\s+(\w+))?",
                select_fields,
                re.IGNORECASE,
            )
            logger.info("Found geojson aliases: %s", geojson_aliases)

            # Add the original function name with any parameters to the list of aliases
            geojson_aliases.append(r"ST_AsGeoJSON\([^\)]*\)")

            # Construct regex to remove GROUP BY clauses for these aliases
            for alias in geojson_aliases:
                if alias:
                    # Ensure we match the alias exactly, followed by space or end of string
                    regex = re.compile(rf"GROUP BY\s+{alias}(?=\s|$)", re.IGNORECASE)
                    query = re.sub(regex, "", query)

        return query

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

        # Pattern to match FROM, JOIN, and IN with schema.table or schema
        pattern = re.compile(
            r"(?P<keyword>FROM|JOIN|IN)\s+(?P<name>\w+(\.\w+)?)", re.IGNORECASE
        )

        # Find all matches for the pattern
        matches = pattern.findall(statement)
        for match in matches:
            keyword, name, _ = match
            if (
                not name.startswith("wherobots_open_data")
                and not name.startswith("wherobots")
                and not name.startswith("wherobots_pro_data")
            ):
                modified_name = f"wherobots_open_data.{name}"
                statement = statement.replace(
                    f"{keyword} {name}", f"{keyword} {modified_name}"
                )

        logger.info(
            "uses WherobotsEngineSpec - _append_catalog_name() - return statement - %s}",
            statement,
        )
        return statement

    @classmethod
    def epoch_to_dttm(cls) -> str:
        raise NotImplementedError()
