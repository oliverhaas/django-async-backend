from django.core.signals import request_started
from django.db import reset_queries
from django.test.utils import CaptureQueriesContext


class AsyncCaptureQueriesContext(CaptureQueriesContext):
    """Async-capable counterpart to Django's CaptureQueriesContext.

    Inherits __init__, __iter__, __getitem__, __len__, and captured_queries
    from Django; adds __aenter__ / __aexit__ for async usage.
    """

    async def __aenter__(self):
        self.force_debug_cursor = self.connection.force_debug_cursor
        self.connection.force_debug_cursor = True
        # Run any initialization queries if needed so that they won't be
        # included as part of the count.
        await self.connection.aensure_connection()
        self.initial_queries = len(self.connection.queries_log)
        self.final_queries = None
        self.reset_queries_disconnected = request_started.disconnect(reset_queries)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.connection.force_debug_cursor = self.force_debug_cursor
        if self.reset_queries_disconnected:
            request_started.connect(reset_queries)
        if exc_type is not None:
            return
        self.final_queries = len(self.connection.queries_log)
