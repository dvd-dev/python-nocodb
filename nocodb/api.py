from typing import Optional
from enum import Enum
from .nocodb import (
    NocoDBProject,
    NocoDBTable,
    NocoDBColumn
)


class NocoDBAPIUris(Enum):
    V1_DB_DATA_PREFIX = "api/v1/db/data"
    V1_DB_META_PREFIX = "api/v1/db/meta"


class NocoDBAPI:
    def __init__(self, base_uri: str):
        self.__base_data_uri = (
            f"{base_uri}/{NocoDBAPIUris.V1_DB_DATA_PREFIX.value}"
        )
        self.__base_meta_uri = (
            f"{base_uri}/{NocoDBAPIUris.V1_DB_META_PREFIX.value}"
        )

    def get_table_uri(self, project: NocoDBProject, table: NocoDBTable) -> str:
        return "/".join(
            (
                self.__base_data_uri,
                project.org_name,
                project.name,
                table.title,
            )
        )

    def get_table_meta_uri(self, table: Optional[NocoDBTable] = None) -> str:
        uri = [
           self.__base_meta_uri,
           "tables",
        ]
        if table:
            uri.append(table.id)

        return "/".join(uri)

    def get_column_meta_uri(self, table: NocoDBTable, column: Optional[NocoDBColumn] = None) -> str:
        uri = [
           self.get_table_meta_uri(table),
            "columns"
        ]
        uri = [
            self.__base_meta_uri,
            "columns",
        ]
        if column:
            uri.append(column.id)
        return "/".join(uri)


    def get_row_detail_uri(
        self, project: NocoDBProject, table: NocoDBTable, row_id: int
    ):
        return "/".join(
            (
                self.__base_data_uri,
                project.org_name,
                project.name,
                table.title,
                str(row_id),
            )
        )

    def get_nested_relations_rows_list_uri(
        self,
        project: NocoDBProject,
        table: NocoDBTable,
        relation_type: str,
        row_id: int,
        column_name: str,
    ) -> str:
        return "/".join(
            (
                self.__base_data_uri,
                project.org_name,
                project.name,
                table.title,
                str(row_id),
                relation_type,
                column_name,
            )
        )

    def get_project_uri(
        self,
        project: NocoDBProject = None
    ) -> str:
        uri = [
            self.__base_meta_uri,
            "projects"
        ]
        if project:
            uri.append(project.id)

        return "/".join(uri)
