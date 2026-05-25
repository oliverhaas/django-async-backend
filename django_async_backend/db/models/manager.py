"""
Manager that exposes our async-capable QuerySet.

Usage (preferred):
    from django_async_backend.db.models import Model, Manager

    class MyModel(Model):
        objects = Manager()  # async methods available on .objects

Legacy alias `AsyncManager` continues to work for code that declares
`async_object = AsyncManager()` alongside Django's default `objects`.

For third-party models you can't modify, see `enable_async` /
`enable_async_globally` for monkey-patch helpers.
"""

from django.db.models.manager import BaseManager

from django_async_backend.db.models.query import QuerySet


class Manager(BaseManager.from_queryset(QuerySet)):  # type: ignore[misc]
    """Manager whose querysets support both Django's sync API and our async API."""


# Backward-compat alias used by existing models as `async_object = AsyncManager()`.
AsyncManager = Manager


def enable_async(model):
    """Make `model.objects` (and all declared managers) return our QuerySet.

    Use for third-party models you can't modify to subclass our Model:

        from django_async_backend.db.models import enable_async
        from some_pkg.models import ThirdPartyModel

        enable_async(ThirdPartyModel)
        # Now: await ThirdPartyModel.objects.aget(pk=1)  -- real async I/O
    """
    for manager in model._meta.managers:
        manager._queryset_class = QuerySet


def enable_async_globally():
    """Patch Django's BaseManager so every model returns our QuerySet by default.

    Invasive. Affects every model in the project, including third-party.
    Call from an app's `ready()` hook. Intended as a transitional helper
    until async support lands in Django proper.
    """
    BaseManager._queryset_class = QuerySet
