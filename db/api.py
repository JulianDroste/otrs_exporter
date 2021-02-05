from typing import Callable, List, Dict, Any
import mysql.connector
from mysql.connector import InterfaceError
from mysql.connector.connection import MySQLCursor
import os
import logging
import settings

LOG = logging.getLogger(__name__)


def _connect_db() -> mysql.connector.connection:
    if os.getenv("OTRS_EXP_DB_PW"):
        return mysql.connector.connect(
            host=settings.DB_HOST,
            database=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PW
        )
    else:
        LOG.warning("No password for database provided in OTRS_EXP_DB_PW! Skipping DB connection!")


db: mysql.connector.connection = _connect_db()


def auto_cursor(func: Callable) -> Callable:
    """
    A function decorator which passes `cursor: PostgresCursor` to the function,
    checks whether `db` is currently connected (and reconnects, if necessary),
    closes `cursor` again and executed `db.commit()` after everything is done.
    """
    def wrapper(*args, **kwargs):
        global db
        if db.closed:
            print("Database is not connected! Attempting reconnect...")
            db = _connect_db()

        if "cursor" in kwargs.keys():
            result = func(*args, **kwargs)
        else:
            with db.cursor() as c:
                result = func(*args, **kwargs, cursor=c)
                try:
                    # Fetch all remaining results, otherwise an error is raised by MySQL, in case the user didn't fetch
                    c.fetchall()
                except mysql.connector.ProgrammingError:
                    # If no records were present, this exception is raised. Everything is fine in this case.
                    pass
        db.commit()
        return result

    return wrapper


@auto_cursor
def _action_where(action: str, table_name: str, **kwargs) -> List[Dict[str, Any]]:
    """
        Executes a `SELECT` or `DELETE` (or any other SQL action statement) in the form of `<action> * FROM table_name WHERE ...`
        and returns the result. Search terms are combined as conjunction with `AND`.
        If the table doesn't exist, the result will be an empty list.
        :param action: Either "select" or "delete". Everything else, raises a value error.
        :param table_name: Name of the table to be searched.
        :param kwargs: Keys are column names and values are search terms. Wildcards are not supported, as they are escaped.
        :return: A `list` containing `dict`s, each mapping column names to values of a data row. The list might be empty.
        """
    cursor = kwargs.pop("cursor")
    if not cursor or not isinstance(cursor, mysql.connector.cursor):
        print("No cursor provided!")
        raise ValueError("No valid PostgresCursor provided by @auto_cursor or user!")
    if action is None or action.upper() not in ["SELECT", "DELETE"]:
        raise ValueError(f"Invalid action requested! Must be SELECT or DELETE! Got {action}.")
    sql_where_clauses = [f'{k} = %s' for k in kwargs.keys()]
    sql_statement = f'{action} {"*" if action.upper() == "SELECT" else ""} FROM {table_name}' \
                    f'{" WHERE " if len(kwargs) > 0 else ";"}' \
                    f'{" AND ".join(sql_where_clauses)};'

    try:
        cursor.execute(sql_statement, tuple(kwargs.values()))
    except mysql.connector.errors.Error as e:
        return []
    search_results = None
    try:
        search_results = cursor.fetchall()
        # Map each result tuple (= 1 row in the DB table) to dict with column names as keys
        col_names = get_db_columns(table_name)
        search_results = list(map(
            lambda sr: {col_names[i]: sr[i] for i in range(len(col_names))},
            search_results
        ))
    except mysql.connector.ProgrammingError:
        pass
    return search_results if search_results is not None else []


def select_where(table_name: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Executes a SELECT query on table `table_name`. Any keyword arguments are used as WHERE clauses, where keys are
    column names and values are "=" comparisons (i.e. ("t", foo=bar) becomes SELECT * from t WHERE foo=bar;).
    If no keyword arguments are specified, the entire table will be returned (often a good idea, as Python is probably
    faster than the database, plus SQL is a pain to write more complex functions with).

    :param table_name: Table on which the query should be executed
    :param kwargs: Any additional search terms
    :return: List of `dict`s, where each `dict` represents a row from the table. Keys are column names.
    """
    return _action_where("SELECT", table_name, **kwargs)


def delete_where(table_name: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Works identical to `select_where` and `_action_where` (with WHERE as action), except it deletes records,
    instead of just returning them.

    :param table_name: Table on which the query should be executed
    :param kwargs: Any additional search terms
    :return: List of `dict`s, where each `dict` represents a deleted row from the table. Keys are column names.
    """
    return _action_where("DELETE", table_name, **kwargs)


@auto_cursor
def get_db_columns(table_name: str, **kwargs):
    cursor = kwargs.pop("cursor")
    if not cursor or not isinstance(cursor, mysql.connector.cursor):
        print("No cursor provided!")
        raise ValueError("No valid PostgresCursor provided by @auto_cursor or user!")
    cursor.execute(f"SELECT column_name FROM INFORMATION_SCHEMA. COLUMNS WHERE TABLE_NAME = '{table_name}';")
    return [column_tuple[0] for column_tuple in cursor.fetchall()]
