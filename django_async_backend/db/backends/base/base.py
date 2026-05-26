import _thread
import copy
import datetime
import logging
import threading
import time
import warnings
import zoneinfo
from collections import deque
from contextlib import (
    asynccontextmanager,
    contextmanager,
)

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    DEFAULT_DB_ALIAS,
    DatabaseError,
    NotSupportedError,
)
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.signals import connection_created
from django.db.backends.utils import debug_transaction
from django.db.transaction import TransactionManagementError
from django.db.utils import ProgrammingError
from django.utils.functional import cached_property

from django_async_backend.db.backends.utils import (
    AsyncCursorDebugWrapper,
    AsyncCursorWrapper,
)
from django_async_backend.db.utils import DatabaseErrorWrapper
from django_async_backend.utils.await_maybe import await_maybe

NO_DB_ALIAS = "__no_db__"
RAN_DB_VERSION_CHECK = set()

logger = logging.getLogger("django_async_backend.db.backends.base")


class BaseAsyncDatabaseWrapper(BaseDatabaseWrapper):
    """Represent an async database connection.

    Subclasses Django's BaseDatabaseWrapper so sync utility methods
    (validate_thread_sharing, execute_wrapper, etc.) are inherited.
    Async I/O methods (aconnect, aclose, acommit, etc.) use a-prefix
    to avoid shadowing Django's sync names.
    """

    # Mapping of Field objects to their column types.
    data_types = {}
    # Mapping of Field objects to their SQL suffix such as AUTOINCREMENT.
    data_types_suffix = {}
    # Mapping of Field objects to their SQL for CHECK constraints.
    data_type_check_constraints = {}
    ops = None
    vendor = "unknown"
    display_name = "unknown"
    features_class = None
    introspection_class = None
    ops_class = None

    queries_limit = 9000

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        # Connection related attributes.
        # The underlying database connection.
        self.connection = None
        # `settings_dict` should be a dictionary containing keys such as
        # NAME, USER, etc. It's called `settings_dict` instead of `settings`
        # to disambiguate it from Django settings modules.
        self.settings_dict = settings_dict
        self.alias = alias
        # Query logging in debug mode or when explicitly enabled.
        self.queries_log = deque(maxlen=self.queries_limit)
        self.force_debug_cursor = False

        # Transaction related attributes.
        # Tracks if the connection is in autocommit mode. Per PEP 249, by
        # default, it isn't.
        self.autocommit = False
        # Tracks if the connection is in a transaction managed by 'atomic'.
        self.in_atomic_block = False
        # Increment to generate unique savepoint ids.
        self.savepoint_state = 0
        # List of savepoints created by 'atomic'.
        self.savepoint_ids = []
        # Stack of active 'atomic' blocks.
        self.atomic_blocks = []
        # Tracks if the outermost 'atomic' block should commit on exit,
        # ie. if autocommit was active on entry.
        self.commit_on_exit = True
        # Tracks if the transaction should be rolled back to the next
        # available savepoint because of an exception in an inner block.
        self.needs_rollback = False
        self.rollback_exc = None

        # Connection termination related attributes.
        self.close_at = None
        self.closed_in_transaction = False
        self.errors_occurred = False
        self.health_check_enabled = False
        self.health_check_done = False

        # Thread-safety related attributes.
        self._thread_sharing_lock = threading.Lock()
        self._thread_sharing_count = 0
        self._thread_ident = _thread.get_ident()

        # A list of no-argument functions to run when the transaction commits.
        # Each entry is an (sids, func, robust) tuple, where sids is a set of
        # the active savepoint IDs when this function was registered and robust
        # specifies whether it's allowed for the function to fail.
        self.run_on_commit = []

        # Should we run the on-commit hooks the next time set_autocommit(True)
        # is called?
        self.run_commit_hooks_on_set_autocommit_on = False

        # A stack of wrappers to be invoked around execute()/executemany()
        # calls. Each entry is a function taking five arguments: execute, sql,
        # params, many, and context. It's the function's responsibility to
        # call execute(sql, params, many, context).
        self.execute_wrappers = []

        self.features = self.features_class(self)
        self.ops = self.ops_class(self)
        if self.introspection_class:
            self.introspection = self.introspection_class(self)

    async def aensure_timezone(self):
        """
        Ensure the connection's timezone is set to `self.timezone_name` and
        return whether it changed or not.
        """
        return False

    @cached_property
    def timezone(self):
        """
        Return a tzinfo of the database connection time zone.

        This is only used when time zone support is enabled. When a datetime is
        read from the database, it is always returned in this time zone.

        When the database backend supports time zones, it doesn't matter which
        time zone Django uses, as long as aware datetimes are used everywhere.
        Other users connecting to the database can choose their own time zone.

        When the database backend doesn't support time zones, the time zone
        Django uses may be constrained by the requirements of other users of
        the database.
        """
        if not settings.USE_TZ:
            return None
        if self.settings_dict["TIME_ZONE"] is None:
            return datetime.UTC
        return zoneinfo.ZoneInfo(self.settings_dict["TIME_ZONE"])

    @cached_property
    def timezone_name(self):
        """
        Name of the time zone of the database connection.
        """
        if not settings.USE_TZ:
            return settings.TIME_ZONE
        if self.settings_dict["TIME_ZONE"] is None:
            return "UTC"
        return self.settings_dict["TIME_ZONE"]

    @property
    def queries_logged(self):
        return self.force_debug_cursor or settings.DEBUG

    @property
    def queries(self):
        if len(self.queries_log) == self.queries_log.maxlen:
            warnings.warn(
                f"Limit for query logging exceeded, only the last {self.queries_log.maxlen} queries will be returned.",
                stacklevel=2,
            )
        return list(self.queries_log)

    async def aget_database_version(self):
        """Return a tuple of the database's version."""
        raise NotImplementedError("subclasses of BaseAsyncDatabaseWrapper may require a get_database_version() method.")

    async def acheck_database_version_supported(self):
        """
        Raise an error if the database version isn't supported by this
        version of Django.
        """
        if (
            self.features.minimum_database_version is not None
            and await self.aget_database_version() < self.features.minimum_database_version
        ):
            db_version = ".".join(map(str, await self.aget_database_version()))
            min_db_version = ".".join(map(str, self.features.minimum_database_version))
            raise NotSupportedError(f"{self.display_name} {min_db_version} or later is required (found {db_version}).")

    # ##### Backend-specific methods for creating connections and cursors #####

    def get_connection_params(self):
        """Return a dict of parameters suitable for get_new_connection."""
        raise NotImplementedError("subclasses of BaseAsyncDatabaseWrapper may require a get_connection_params() method")

    async def aget_new_connection(self, conn_params):
        """Open a connection to the database."""
        raise NotImplementedError("subclasses of BaseAsyncDatabaseWrapper may require a get_new_connection() method")

    async def ainit_connection_state(self):
        """Initialize the database connection settings."""
        if self.alias not in RAN_DB_VERSION_CHECK:
            await self.acheck_database_version_supported()
            RAN_DB_VERSION_CHECK.add(self.alias)

    def create_cursor(self, name=None):
        """Create a cursor. Assume that a connection is established."""
        raise NotImplementedError("subclasses of BaseAsyncDatabaseWrapper may require a create_cursor() method")

    # ##### Backend-specific methods for creating connections #####

    async def aconnect(self):
        """Connect to the database. Assume that the connection is closed."""
        # Check for invalid configurations.
        self.check_settings()
        # In case the previous connection was closed while in an atomic block
        self.in_atomic_block = False
        self.savepoint_ids = []
        self.atomic_blocks = []
        self.needs_rollback = False
        # Reset parameters defining when to close/health-check the connection.
        self.health_check_enabled = self.settings_dict["CONN_HEALTH_CHECKS"]
        max_age = self.settings_dict["CONN_MAX_AGE"]
        self.close_at = None if max_age is None else time.monotonic() + max_age
        self.closed_in_transaction = False
        self.errors_occurred = False
        # New connections are healthy.
        self.health_check_done = True
        # Establish the connection
        conn_params = self.get_connection_params()
        self.connection = await self.aget_new_connection(conn_params)
        await self.aset_autocommit(self.settings_dict["AUTOCOMMIT"])
        await self.ainit_connection_state()
        connection_created.send(sender=self.__class__, connection=self)

        self.run_on_commit = []

    async def aensure_connection(self):
        """Guarantee that a connection to the database is established."""
        if self.connection is None:
            if self.in_atomic_block and self.closed_in_transaction:
                raise ProgrammingError("Cannot open a new connection in an atomic block.")
            with self.wrap_database_errors:
                await self.aconnect()

    # ##### Backend-specific wrappers for PEP-249 connection methods #####

    async def _acursor(self, name=None):
        await self.aclose_if_health_check_failed()
        await self.aensure_connection()

        with self.wrap_database_errors:
            return self._prepare_cursor(self.create_cursor(name))

    async def _acommit(self):
        if self.connection is not None:
            with debug_transaction(self, "COMMIT"), self.wrap_database_errors:
                return await self.connection.commit()

    async def _arollback(self):
        if self.connection is not None:
            with debug_transaction(self, "ROLLBACK"), self.wrap_database_errors:
                return await self.connection.rollback()

    async def _aclose(self):
        if self.connection is not None:
            with self.wrap_database_errors:
                return await self.connection.close()

    # ##### Generic wrappers for PEP-249 connection methods #####

    def acursor(self):
        """Create a cursor, opening a connection if necessary."""
        return self._acursor()

    async def acommit(self):
        """Commit a transaction and reset the dirty flag."""
        self.validate_thread_sharing()
        self.validate_no_atomic_block()
        await self._acommit()
        # A successful commit means that the database connection works.
        self.errors_occurred = False
        self.run_commit_hooks_on_set_autocommit_on = True

    async def arollback(self):
        """Roll back a transaction and reset the dirty flag."""
        self.validate_thread_sharing()
        self.validate_no_atomic_block()
        await self._arollback()
        # A successful rollback means that the database connection works.
        self.errors_occurred = False
        self.needs_rollback = False
        self.run_on_commit = []

    async def aclose(self):
        """Close the connection to the database."""
        self.validate_thread_sharing()
        self.run_on_commit = []

        # Don't call validate_no_atomic_block() to avoid making it difficult
        # to get rid of a connection in an invalid state. The next connect()
        # will reset the transaction state anyway.
        if self.closed_in_transaction or self.connection is None:
            return
        try:
            await self._aclose()
        finally:
            if self.in_atomic_block:
                self.closed_in_transaction = True
                self.needs_rollback = True
            else:
                self.connection = None

    # ##### Backend-specific savepoint management methods #####

    async def _asavepoint(self, sid):
        async with await self.acursor() as cursor:
            await cursor.execute(self.ops.savepoint_create_sql(sid))

    async def _asavepoint_rollback(self, sid):
        async with await self.acursor() as cursor:
            await cursor.execute(self.ops.savepoint_rollback_sql(sid))

    async def _asavepoint_commit(self, sid):
        async with await self.acursor() as cursor:
            await cursor.execute(self.ops.savepoint_commit_sql(sid))

    async def _asavepoint_allowed(self):
        # Savepoints cannot be created outside a transaction
        return self.features.uses_savepoints and not await self.aget_autocommit()

    # ##### Generic savepoint management methods #####

    async def asavepoint(self):
        """
        Create a savepoint inside the current transaction. Return an
        identifier for the savepoint that will be used for the subsequent
        rollback or commit. Do nothing if savepoints are not supported.
        """
        if not await self._asavepoint_allowed():
            return None

        thread_ident = _thread.get_ident()
        tid = str(thread_ident).replace("-", "")

        self.savepoint_state += 1
        sid = "s%s_x%d" % (tid, self.savepoint_state)

        self.validate_thread_sharing()
        await self._asavepoint(sid)

        return sid

    async def asavepoint_rollback(self, sid):
        """
        Roll back to a savepoint. Do nothing if savepoints are not supported.
        """
        if not await self._asavepoint_allowed():
            return

        self.validate_thread_sharing()
        await self._asavepoint_rollback(sid)

        # Remove any callbacks registered while this savepoint was active.
        self.run_on_commit = [(sids, func, robust) for (sids, func, robust) in self.run_on_commit if sid not in sids]

    async def asavepoint_commit(self, sid):
        """
        Release a savepoint. Do nothing if savepoints are not supported.
        """
        if not await self._asavepoint_allowed():
            return

        self.validate_thread_sharing()
        await self._asavepoint_commit(sid)

    def clean_savepoints(self):
        """
        Reset the counter used to generate unique savepoint ids in this thread.
        """
        self.savepoint_state = 0

    # ##### Backend-specific transaction management methods #####

    def _aset_autocommit(self, autocommit):
        """
        Backend-specific implementation to enable or disable autocommit.
        """
        raise NotImplementedError("subclasses of BaseAsyncDatabaseWrapper may require a _set_autocommit() method")

    # ##### Generic transaction management methods #####

    async def aget_autocommit(self):
        """Get the autocommit state."""
        await self.aensure_connection()
        return self.autocommit

    async def aset_autocommit(self, autocommit, force_begin_transaction_with_broken_autocommit=False):
        """
        Enable or disable autocommit.

        The usual way to start a transaction is to turn autocommit off.
        SQLite does not properly start a transaction when disabling
        autocommit. To avoid this buggy behavior and to actually enter a new
        transaction, an explicit BEGIN is required. Using
        force_begin_transaction_with_broken_autocommit=True will issue an
        explicit BEGIN with SQLite. This option will be ignored for other
        backends.
        """
        self.validate_no_atomic_block()
        await self.aclose_if_health_check_failed()
        await self.aensure_connection()

        start_transaction_under_autocommit = (
            force_begin_transaction_with_broken_autocommit
            and not autocommit
            and hasattr(self, "_start_transaction_under_autocommit")
        )

        if start_transaction_under_autocommit:
            await self._start_transaction_under_autocommit()
        elif autocommit:
            await self._aset_autocommit(autocommit)
        else:
            with debug_transaction(self, "BEGIN"):
                await self._aset_autocommit(autocommit)
        self.autocommit = autocommit

        if autocommit and self.run_commit_hooks_on_set_autocommit_on:
            await self.arun_and_clear_commit_hooks()
            self.run_commit_hooks_on_set_autocommit_on = False

    # ##### Connection termination handling #####

    async def ais_usable(self):
        """
        Test if the database connection is usable.

        This method may assume that self.connection is not None.

        Actual implementations should take care not to raise exceptions
        as that may prevent Django from recycling unusable connections.
        """
        raise NotImplementedError("subclasses of BaseAsyncDatabaseWrapper may require an is_usable() method")

    async def aclose_if_health_check_failed(self):
        """Close existing connection if it fails a health check."""
        if self.connection is None or not self.health_check_enabled or self.health_check_done:
            return

        if not await self.ais_usable():
            await self.aclose()
        self.health_check_done = True

    async def aclose_if_unusable_or_obsolete(self):
        """
        Close the current connection if unrecoverable errors have occurred
        or if it outlived its maximum age.
        """
        if self.connection is not None:
            self.health_check_done = False
            # If the application didn't restore the original autocommit
            # setting, don't take chances, drop the connection.
            if await self.aget_autocommit() != self.settings_dict["AUTOCOMMIT"]:
                await self.aclose()
                return

            # If an exception other than DataError or IntegrityError occurred
            # since the last commit / rollback, check if the connection works.
            if self.errors_occurred:
                if await self.ais_usable():
                    self.errors_occurred = False
                    self.health_check_done = True
                else:
                    await self.aclose()
                    return

            if self.close_at is not None and time.monotonic() >= self.close_at:
                await self.aclose()
                return

    # ##### Thread safety handling #####

    @property
    def allow_thread_sharing(self):
        with self._thread_sharing_lock:
            return self._thread_sharing_count > 0

    # ##### Miscellaneous #####

    @cached_property
    def wrap_database_errors(self):
        """
        Context manager and decorator that re-throws backend-specific database
        exceptions using Django's common wrappers.
        """
        return DatabaseErrorWrapper(self)

    def achunked_cursor(self):
        """
        Return a cursor that tries to avoid caching in the database (if
        supported by the database), otherwise return a regular cursor.
        """
        return self.acursor()

    def make_debug_cursor(self, cursor):
        """Create a cursor that logs all queries in self.queries_log."""
        return AsyncCursorDebugWrapper(cursor, self)

    def make_cursor(self, cursor):
        """Create a cursor without debug logging."""
        return AsyncCursorWrapper(cursor, self)

    @asynccontextmanager
    async def atemporary_connection(self):
        """
        Context manager that ensures that a connection is established, and
        if it opened one, closes it to avoid leaving a dangling connection.
        This is useful for operations outside of the request-response cycle.

        Provide a cursor: with self.atemporary_connection() as cursor: ...
        """
        must_close = self.connection is None
        try:
            async with await self.acursor() as cursor:
                yield cursor
        finally:
            if must_close:
                await self.aclose()

    async def aon_commit(self, func, robust=False):
        if not callable(func):
            raise TypeError("on_commit()'s callback must be a callable.")
        if self.in_atomic_block:
            # Transaction in progress; save for execution on commit.
            self.run_on_commit.append((set(self.savepoint_ids), func, robust))
        elif not await self.aget_autocommit():
            raise TransactionManagementError("on_commit() cannot be used in manual transaction management")
        # No transaction in progress and in autocommit mode; execute
        # immediately.
        elif robust:
            try:
                await await_maybe(func())
            except Exception as e:
                logger.error(
                    f"Error calling {func.__qualname__} in on_commit() (%s).",
                    e,
                    exc_info=True,
                )
        else:
            await await_maybe(func())

    async def arun_and_clear_commit_hooks(self):
        self.validate_no_atomic_block()
        current_run_on_commit = self.run_on_commit
        self.run_on_commit = []
        while current_run_on_commit:
            _, func, robust = current_run_on_commit.pop(0)
            if robust:
                try:
                    await await_maybe(func())
                except Exception as e:
                    logger.error(
                        f"Error calling {func.__qualname__} in on_commit() during transaction (%s).",
                        e,
                        exc_info=True,
                    )
            else:
                await await_maybe(func())
