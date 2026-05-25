"""
Verify the unified Model + Manager API: a single base class gives both sync
and async access on the default `objects` attribute, with no `async_object`
boilerplate.
"""

from django.db.models.manager import BaseManager
from django.db.models.query import QuerySet as DjangoQuerySet

from django_async_backend.db.models import (
    AsyncManager,
    AsyncModelMixin,
    Manager,
    Model,
    QuerySet,
    enable_async,
    enable_async_globally,
)


def test_queryset_is_a_django_queryset():
    assert issubclass(QuerySet, DjangoQuerySet)


def test_manager_subclasses_base_manager_and_wires_our_queryset():
    assert issubclass(Manager, BaseManager)
    assert Manager._queryset_class is QuerySet


def test_model_includes_async_mixin_and_django_model():
    import django.db.models

    assert issubclass(Model, AsyncModelMixin)
    assert issubclass(Model, django.db.models.Model)


def test_backward_compat_aliases():
    assert AsyncManager is Manager


def test_enable_async_helpers_exposed():
    assert callable(enable_async)
    assert callable(enable_async_globally)
