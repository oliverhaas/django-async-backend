import asyncio

from asgiref.sync import iscoroutinefunction
from django.db.utils import ConnectionHandler, load_backend
from django.db.utils import DatabaseErrorWrapper as _DatabaseErrorWrapper

from django_async_backend.utils.connection import BaseAsyncConnectionHandler


class DatabaseErrorWrapper(_DatabaseErrorWrapper):
    def __call__(self, func):
        # Note that we are intentionally not using @wraps here for performance
        # reasons. Refs #21109.
        if iscoroutinefunction(func):

            async def inner(*args, **kwargs):
                with self:
                    return await func(*args, **kwargs)

        else:

            def inner(*args, **kwargs):
                with self:
                    return func(*args, **kwargs)

        return inner


class AsyncConnectionHandler(BaseAsyncConnectionHandler, ConnectionHandler):
    # settings_name, thread_critical, and configure_settings are all
    # inherited from Django's ConnectionHandler via multiple inheritance.
    # We only override create_connection to return our async wrapper and
    # track per-task ownership for cross-task transaction safety.

    def create_connection(self, alias):
        db = self.settings[alias]
        backend = load_backend(db["ENGINE"])

        if not hasattr(backend, "AsyncDatabaseWrapper"):
            raise self.exception_class(f"The async connection '{alias}' doesn't exist.")

        wrapper = backend.AsyncDatabaseWrapper(db, alias)
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        wrapper._task_connection_owner = id(task) if task else None
        return wrapper
