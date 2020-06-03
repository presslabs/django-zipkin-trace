from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import CursorWrapper
from py_zipkin.zipkin import zipkin_client_span


class ZipkinCursorWrapper(CursorWrapper):
    def execute(self, sql, params=()):
        with zipkin_client_span(
            service_name=self.get_db_type(),
            span_name=self.get_operation(sql),
            binary_annotations=self.get_binary_annotations(sql),
        ) as span:
            self.add_sa_binary_annotation(span)
            return self.cursor.execute(sql, params)

    def executemany(self, sql, param_list):
        with zipkin_client_span(
            service_name=self.get_db_type(),
            span_name=self.get_operation(sql),
            binary_annotations=self.get_binary_annotations(sql),
        ) as span:
            self.add_sa_binary_annotation(span)
            return self.cursor.executemany(sql, param_list)

    def get_db_type(self):
        try:
            return self.db.vendor
        except:
            return "SQL"

    def get_operation(self, statement):
        space_idx = statement.find(" ")
        if space_idx == -1:
            operation = "db-query"  # unrecognized format of the query
        else:
            operation = statement[0:space_idx]
        return operation

    def get_binary_annotations(self, sql):
        out = {
            "db.instance": self.db.alias,
            "db.statement": sql,
            "db.type": self.get_db_type().lower(),
        }

        try:
            out["db.user"] = self.db.settings_dict["USER"]
        except KeyError:
            pass

        return out

    def add_sa_binary_annotation(self, span):
        default_port = 0

        db_class = self.db.settings_dict.get("ENGINE", "").lower()
        if "mysql" in db_class:
            default_port = 3306
        elif "postgres" in db_class:
            default_port = 5432

        host = self.db.settings_dict.get("HOST", "") or "localhost"
        port = int(self.db.settings_dict.get("PORT", default_port) or default_port)
        span.add_sa_binary_annotation(port, host)


def wrapped_cursor(original):
    def wrapped(self, *args, **kwargs):
        cursor = original(self, *args, **kwargs)
        return ZipkinCursorWrapper(cursor, self)

    return wrapped


def init():
    BaseDatabaseWrapper.cursor = wrapped_cursor(BaseDatabaseWrapper.cursor)
