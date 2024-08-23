"""Utilities for REST API."""
import contextlib
import dataclasses
import enum
import functools
import json
import os
import pathlib
import sqlite3
import traceback
from typing import Any, Dict, List, Optional, Tuple, Union

import filelock

from sky.api.requests import decoders
from sky.api.requests import encoders
from sky.utils import common_utils
from sky.utils import db_utils

TASK_LOG_PATH_PREFIX = '~/sky_logs/requests'


class RequestStatus(enum.Enum):
    """The status of a task."""

    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    ABORTED = 'ABORTED'

    def __gt__(self, other):
        return (list(RequestStatus).index(self) >
                list(RequestStatus).index(other))


REQUEST_TASK_COLUMNS = [
    'request_id',
    'name',
    'entrypoint',
    'request_body',
    'status',
    'return_value',
    'error',
    'pid',
]


@dataclasses.dataclass
class RequestTaskPayload:
    request_id: str
    name: str
    entrypoint: str
    request_body: str
    status: str
    return_value: str
    error: str
    pid: Optional[int]


@dataclasses.dataclass
class RequestTask:
    """A REST task."""

    request_id: str
    name: str
    entrypoint: str
    request_body: Dict[str, Any]
    status: RequestStatus
    return_value: Any = None
    error: Optional[Dict[str, Any]] = None
    pid: Optional[int] = None

    @property
    def log_path(self) -> pathlib.Path:
        log_path_prefix = pathlib.Path(
            TASK_LOG_PATH_PREFIX).expanduser().absolute()
        log_path_prefix.mkdir(parents=True, exist_ok=True)
        log_path = (log_path_prefix / self.request_id).with_suffix('.log')
        return log_path

    def set_error(self, error: Exception) -> None:
        """Set the error."""

        self.error = {
            'object': encoders._pickle_and_encode(error),
            'type': type(error).__name__,
            'message': str(error),
        }

    def get_error(self) -> Optional[Dict[str, Any]]:
        """Get the error."""
        if self.error is None:
            return None
        return {
            'object': decoders._decode_and_unpickle(self.error['object']),
            'type': self.error['type'],
            'message': self.error['message'],
        }

    def set_return_value(self, return_value: Any) -> None:
        """Set the return value."""
        self.return_value = encoders.get_handler(self.name)(return_value)

    def get_return_value(self) -> Any:
        """Get the return value."""
        return decoders.get_handler(self.name)(self.return_value)

    @classmethod
    def from_row(cls, row: Tuple[Any, ...]) -> 'RequestTask':
        return cls.decode(
            RequestTaskPayload(**dict(zip(REQUEST_TASK_COLUMNS, row))))

    def to_row(self) -> Tuple[Any, ...]:
        payload = self.encode()
        return tuple(getattr(payload, k) for k in REQUEST_TASK_COLUMNS)

    def encode(self) -> RequestTaskPayload:
        """Serialize the request task."""
        try:
            return RequestTaskPayload(
                request_id=self.request_id,
                name=self.name,
                entrypoint=self.entrypoint,
                request_body=json.dumps(self.request_body),
                status=self.status.value,
                return_value=json.dumps(self.return_value),
                error=json.dumps(self.error),
                pid=self.pid,
            )
        except TypeError as e:
            print('Error encoding.\n'
                    f'{self.request_id}\n'
                    f'{self.name}\n'
                        f'{self.request_body}\n'
                        f'{self.return_value}\n')
            raise

    @classmethod
    def decode(cls, payload: RequestTaskPayload) -> 'RequestTask':
        """Deserialize the request task."""
        return cls(
            request_id=payload.request_id,
            name=payload.name,
            entrypoint=payload.entrypoint,
            request_body=json.loads(payload.request_body),
            status=RequestStatus(payload.status),
            return_value=json.loads(payload.return_value),
            error=json.loads(payload.error),
            pid=payload.pid,
        )


_DB_PATH = os.path.expanduser('~/.sky/api_server/tasks.db')
pathlib.Path(_DB_PATH).parents[0].mkdir(parents=True, exist_ok=True)


def create_table(cursor, conn):
    del conn
    # Enable WAL mode to avoid locking issues.
    # See: issue #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if not common_utils.is_wsl():
        try:
            cursor.execute('PRAGMA journal_mode=WAL')
        except sqlite3.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    # Table for Clusters
    cursor.execute("""\
        CREATE TABLE IF NOT EXISTS rest_tasks (
        request_id TEXT PRIMARY KEY,
        name TEXT,
        entrypoint TEXT,
        request_body TEXT,
        status TEXT,
        return_value TEXT,
        error BLOB,
        pid INTEGER)""")


_DB = None


def init_db(func):
    """Initialize the database."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global _DB
        if _DB is None:
            _DB = db_utils.SQLiteConn(_DB_PATH, create_table)
        return func(*args, **kwargs)

    return wrapper


def reset_db():
    """Create the database."""
    common_utils.remove_file_if_exists(_DB_PATH)


def request_lock_path(request_id: str) -> str:
    request_lock = os.path.join(os.path.dirname(_DB_PATH),
                                f'.{request_id}.lock')
    return request_lock


@contextlib.contextmanager
@init_db
def update_rest_task(request_id: str):
    """Get a REST task."""
    rest_task = _get_rest_task_no_lock(request_id)
    yield rest_task
    if rest_task is not None:
        _dump_request_no_lock(rest_task)


def _get_rest_task_no_lock(request_id: str) -> Optional[RequestTask]:
    """Get a REST task."""
    assert _DB is not None
    with _DB.conn:
        cursor = _DB.conn.cursor()
        cursor.execute('SELECT * FROM rest_tasks WHERE request_id=?',
                       (request_id,))
        row = cursor.fetchone()
        if row is None:
            return None
    return RequestTask.from_row(row)


@init_db
def get_request(request_id: str) -> Optional[RequestTask]:
    """Get a REST task."""
    with filelock.FileLock(request_lock_path(request_id)):
        return _get_rest_task_no_lock(request_id)


@init_db
def get_request_tasks() -> List[RequestTask]:
    """Get a REST task."""
    assert _DB is not None
    with _DB.conn:
        cursor = _DB.conn.cursor()
        cursor.execute('SELECT * FROM rest_tasks')
        rows = cursor.fetchall()
        if rows is None:
            return []
    rest_tasks = []
    for row in rows:
        rest_task = RequestTask.from_row(row)
        rest_tasks.append(rest_task)
    return rest_tasks


def _dump_request_no_lock(request_task: RequestTask):
    """Dump a REST task."""
    row = request_task.to_row()
    fill_str = ', '.join(['?'] * len(row))
    assert _DB is not None
    with _DB.conn:
        cursor = _DB.conn.cursor()
        cursor.execute(f'INSERT OR REPLACE INTO rest_tasks VALUES ({fill_str})',
                       row)


@init_db
def dump_reqest(request: RequestTask):
    """Dump a REST task."""
    with filelock.FileLock(request_lock_path(request.request_id)):
        _dump_request_no_lock(request)


# def _check_process_alive(pid: int) -> bool:
#     """Check if a process is alive."""

#     psutil_process = psutil.Process(pid)
#     return psutil_process.is_running()
