from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING
from datetime import datetime
if TYPE_CHECKING:
    from .api import NocoDBAPI

"""
License MIT

Copyright 2022 Samuel López Saura

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


class AuthToken(ABC):
    @abstractmethod
    def get_header(self) -> dict:
        pass


class APIToken:
    def __init__(self, token: str):
        self.__token = token

    def get_header(self) -> dict:
        return {"xc-token": self.__token}


class JWTAuthToken:
    def __init__(self, token: str):
        self.__token = token

    def get_header(self) -> dict:
        return {"xc-auth": self.__token}


class WhereFilter(ABC):
    @abstractmethod
    def get_where(self) -> str:
        pass


"""This could be great but actually I don't know how to join filters in the
NocoDB DSL. I event don't know if this is possible through the current API.
I hope they add docs about it soon.

class NocoDBWhere:

    def __init__(self):
        self.__filter_array: List[WhereFilter] = []

    def add_filter(self, where_filter: WhereFilter) -> NocoDBWhere:
        self.__filter_array.append(
                where_filter
        )
        return self

    def get_where(self) -> str:
        return '&'.join([filter_.get_where() for filter_ in self.__filter_array])

    def __str__(self):
        return f'Where: "{self.get_where()}"'
"""

class NocoDBBase:
    _obj: Optional[dict] = None
    _attributes = []
    def update(self, kw, attributes: list = []):
        if not attributes:
            attributes = self._attributes
        self.__dict__.update(
            {k:v for k, v in kw.items() if k in attributes}
        )
        if not self._obj:
            self._obj = kw

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.title}>"


class NocoDBProject(NocoDBBase):
    _attributes = [
        "id",
        "status",
        "description",
        "color",
        "uuid",
        "roles",
        "deleted",
        "created_at",
        "updated_at",
    ]
    def __init__(self, org_name: str, project_name: str, api: "NocoDBAPI"):
        self.name = project_name
        self.org_name = org_name
        self._api = api
        self.tables = []

    def get_tables(self):
        for t in self._api.project_table_list(self).get("list", []):
            table = NocoDBTable(t["table_name"], self, t["title"], self._api)
            table.update(t)
            self.tables.append(table)

    def table_by_name(self, key):
        return next((t for t in self.tables if t.title == key), None)

    def table_by_id(self, key):
        return next((t for t in self.tables if t.id == key), None)


class NocoDBTable(NocoDBBase):
    _attributes = [
        "id",
        "base_id",
        "project_id",
        "enabled",
        "tags",
        "deleted",
        "created_at",
        "updated_at"
    ]
    def __init__(self, table_name: str, project: NocoDBProject, title: str, api: "NocoDBAPI"):
        self.name = table_name
        self.project = project
        self.title = title
        self._api = api
        self.columns = []
        self.rows = []

    def get_columns(self):
        for c in self._api.table_column_list(self).get("columns", []):
            col = NocoDBColumn(c["column_name"], self, c["title"], self._api)
            col.update(c)
            self.columns.append(col)

    def column_by_key(self, key):
        return next((c for c in self.columns if c.title == key), None)

    def column_by_id(self, key):
        return next((c for c in self.columns if c.id == key), None)

    def row_by_key(self, key, value):
        return next((r for r in self.rows if r.get_attr(key) == value), None)

    def row_by_pv(self, value):
        return next((r for r in self.rows if r.primary_value.value == value), None)

    def delete_rows(self, ids=[]):
        if not ids:
            ids = [r.id for r in self.rows]
        for id in ids:
            self._api.table_row_delete(self.project, self, id)

    def get_rows(self):
        rows = self._api.table_row_list(
            self.project, self,
            params={'limit': 10000}).get("list", []
        )
        self.rows = []
        if not rows:
            return
        for row in rows:
            new_row = []
            for k, v in row.items():
                if col := self.column_by_key(k):
                    new_row.append(NocoDBData(k, v, col))
                else:
                    print(f"Mismatched column {k}: {v}")
            self.rows.append(NocoDBRow(self, new_row))

    def _map_columns(self, **kw):
        row = []
        for k, v in kw.items():
            col = self.column_by_key(k)
            value = v
            if not col:
                print(f"No mapped column for {k}")
                continue
            if col.relation:
                #for x,y in col.__dict__.items():
                #    print(f"FK: {x} {y}")
                col.fk_model = self.project.table_by_id(col.fk_related_model_id)
                col.fk_child = col.fk_model.column_by_id(col.fk_child_column_id)
                col.fk_parent = col.fk_model.column_by_id(col.fk_parent_column_id)
                parent_row = col.fk_model.row_by_pv(v)
                if not parent_row:
                    row.append(NocoDBData(k, {}, col, True))
                    row.append(NocoDBData(col.fk_child.title, None, col.fk_child, True))
                    continue
                dict_value = {'Id': parent_row.id, parent_row.primary_key.title: v}
                row.append(NocoDBData(k, dict_value, col, True))
                row.append(NocoDBData(col.fk_child.title, parent_row.id, col.fk_child, True))
                #print(
                #    f"Mapping {col.title} {v} {dict_value} Table: {col.fk_model} Parent: "
                #    f"{col.fk_parent} Child: {col.fk_child} Parent Row: "
                #    f"{parent_row.primary_value.value} {parent_row.id}"
                #)
                continue
            row.append(NocoDBData(k, v, col, True))
        return NocoDBRow(self, row)

    def diff_rows(self, left, **kw):
        right = self._map_columns(**kw)
        diffs = 0
        for l_col in left.data:
            r_col = right.get_col(l_col.title)
            if not r_col and l_col.title not in ["Id", "CreatedAt", "UpdatedAt"]:
                #print(f"Missing property {l_col.title} in mapping")
                continue
            elif not r_col:
                continue
            elif l_col.value != r_col.value:
                #print(f"Mismatched property {l_col.title}: {l_col.value} != {r_col.value}")
                diffs += 1
                l_col._value = r_col._value
        if diffs > 0:
            return left

    def update_row(self, row, **kw):
        if new := self.diff_rows(row, **kw):
            print(f"Updating row {new}")
            self._api.table_row_update(
                self.project,
                self,
                new.id,
                new.json
            )

    def create_row(self, **kw):
        row = self._map_columns(**kw)
        return self._api.table_row_create(
            self.project,
            self, row.json
        )


class NocoDBRow(NocoDBBase):
    def __init__(self, table, data):
        self.table = table
        for col in data:
            col.row = self
        self.data = data
        self.columns = [x.column for x in self.data]

    def get_col(self, key):
        return next((x for x in self.data if x.title == key), None)

    def get_attr(self, key):
        col = self.get_col(key)
        return col.value if col else None

    @property
    def primary_key(self):
        return next((x for x in self.columns if x.pv), None)

    @property
    def primary_value(self):
        return next((x for x in self.data if x.column.pv), None)

    @property
    def title(self):
        return self.primary_value.value

    @property
    def id(self):
        return self.get_attr('Id')

    @property
    def json(self):
        return {c.title: str(c.value) for c in self.data}

    def __eq__(self, other):
        return (
            self.id == other.id and
            self.table == other.table
        )


class NocoDBColumn(NocoDBBase):
    _attributes = [
        "id",
        "base_id",
        "project_id",
        "fk_model_id",
        "pv",
        "uidt",
        "dt",
        "dtx",
        "dtxp",
        "colOptions",
        "options",
        "ref_db_alias",
        "db_type",
        "updated_at",
        "created_at",
        "deleted"
    ]
    _related_attributes = [
        'fk_related_model_id',
        'fk_column_id',
        'fk_child_column_id',
        'fk_parent_column_id',
        'fk_mm_model_id',
        'fk_mm_child_column_id',
        'fk_mm_parent_column_id',
    ]
    def __init__(self, column_name: str, table: NocoDBTable, title: str, api: "NocoDBAPI"):
        self.name = column_name
        self.table = table
        self.title = title
        self._api = api
        self.relation = False
        self.colOptions = {}
        self.options = {}

    def update(self, kw):
        super().update(kw)
        self.options = self.colOptions.get("options", {})
        if self.uidt == "LinkToAnotherRecord":
            self.relation = True
            super().update(self.colOptions, self._related_attributes)

    def option_by_name(self, key):
        return next((k for k in self.options if k["title"] == key), None)

    def add_option(self, key):
        options = self._obj
        options["dtxp"] += f", '{key}'"
        options["colOptions"]["options"].append({
            "title": key,
            "fk_column_id": self.id
        })
        self._obj = options
        self.update(options)
        return self._api.column_update(
            self, options
        )


class NocoDBData(NocoDBBase):
    SELECT_FIELDS = [
        "SingleSelect",
        "MultiSelect",
    ]

    def __init__(self, key, value, column, add_option: bool = False):
        self.title = key
        self._value = value
        self.column = column
        self.row = None
        if add_option:
            self.add_option()

    @property
    def value(self):
        if self.column.dt == "datetime":
            return datetime.fromisoformat(self._value)
        if self.column.dt == "integer":
            return int(self._value or 0)
        if self.column.relation:
            if not self._value:
                return self._value
            if isinstance(self._value, list):
                return [x.get(self.row.primary_value.title) for x in self._value]
            return self._value.get(self.row.primary_value.title)
        if isinstance(self._value, list):
            return ",".join(self._value)
        return self._value

    def add_option(self):
        col = self.column
        if not col.uidt in self.SELECT_FIELDS:
            return
        values = self._value
        if not isinstance(values, list):
            values = [values]
        for value in values:
            if col.option_by_name(value):
                continue
            print(col.add_option(value))

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.title} {self._value}>"


class NocoDBClient:
    @abstractmethod
    def table_row_list(
        self, project: NocoDBProject, table: str, filter_obj=None, params=None
    ) -> dict:
        pass

    @abstractmethod
    def table_row_list(
        self,
        project: NocoDBProject,
        table: str,
        filter_obj: Optional[WhereFilter] = None,
        params: Optional[dict] = None,
    ) -> dict:
        pass

    @abstractmethod
    def table_row_create(
        self, project: NocoDBProject, table: str, body: dict
    ) -> dict:
        pass

    @abstractmethod
    def table_row_detail(
        self, project: NocoDBProject, table: str, row_id: int
    ) -> dict:
        pass

    @abstractmethod
    def table_row_update(
        self, project: NocoDBProject, table: str, row_id: int, body: dict
    ) -> dict:
        pass

    @abstractmethod
    def table_row_delete(
        self, project: NocoDBProject, table: str, row_id: int
    ) -> int:
        pass

    @abstractmethod
    def table_row_nested_relations_list(
        self,
        project: NocoDBProject,
        table: str,
        relation_type: str,
        row_id: int,
        column_name: str,
    ) -> dict:
        pass
