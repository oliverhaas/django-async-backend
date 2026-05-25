# Django Async Backend

> **Note:** This is a fork of [Arfey/django-async-backend](https://github.com/Arfey/django-async-backend), developed with AI assistance to explore the remaining blockers for a fully async Django ORM. The goal is to identify what works, what doesn't, and hopefully contribute something useful upstream.

## Installation & Django Integration

### 1. Install the package

```bash
pip install django-async-backend
```

### 2. Django settings

```python
DATABASES = {
    "default": {
        "ENGINE": "django_async_backend.db.backends.postgresql",
        ...
    },
}

INSTALLED_APPS = [
    ...
    "django_async_backend",
    ...
]
```

---

## Connection Pooling

> Connection pooling is not supported when running under a WSGI server (including the Django development server), because WSGI creates a new event loop for each request. This prevents reliable management of connection pool state.

---

## Async Transactions

```python
from django_async_backend.db.transaction import aatomic

async with aatomic():
    await MyModel.objects.acreate(name="test")
```

Nested `aatomic` blocks create savepoints. Cross-task transaction reuse is detected and raises `RuntimeError`. Use `_independent_connection()` or a parent-level transaction instead.

---

## Model Support

Subclass `Model` to get the async ORM with a single base class. The default `objects` manager is wired up automatically and exposes both Django's sync API and our async API on the same QuerySet.

```python
from django.db import models

from django_async_backend.db.models import Model

class MyModel(Model):
    name = models.CharField(max_length=100)
```

Then:

```python
# Async I/O (real async, uses the async connection pool):
obj = await MyModel.objects.aget(pk=1)
async for row in MyModel.objects.filter(active=True):
    ...

# Sync query construction (inherited from Django, no I/O):
qs = MyModel.objects.filter(active=True).order_by("name")
count = await qs.acount()
```

`Model` validates at class definition time that any subclass overriding `save()` also overrides `asave()` (and `delete()`/`adelete()`), preventing silent logic skipping. Pass `async_mro_strict=False` to opt out.

### Third-party models

For models you can't modify, use the monkey-patch helpers:

```python
from django_async_backend.db.models import enable_async, enable_async_globally
from some_pkg.models import ThirdPartyModel

# Per-model opt-in:
enable_async(ThirdPartyModel)
obj = await ThirdPartyModel.objects.aget(pk=1)

# Global opt-in (call from AppConfig.ready):
enable_async_globally()
```

### Legacy mixin

The `AsyncModelMixin` mixin and `AsyncManager` alias still work for code written before the unified API:

```python
from django_async_backend.db.models.base import AsyncModelMixin
from django_async_backend.db.models.manager import AsyncManager

class MyModel(AsyncModelMixin, models.Model):
    name = models.CharField(max_length=100)
    async_object = AsyncManager()
```

### QuerySet Methods

| Method | Status | Notes |
|--------|--------|-------|
| `aget`, `acreate`, `acount`, `aexists` | Supported | |
| `abulk_create` | Supported | With `ignore_conflicts` and `update_conflicts` |
| `abulk_update`, `aupdate`, `adelete` | Supported | |
| `aget_or_create`, `aupdate_or_create` | Supported | |
| `afirst`, `alast`, `aearliest`, `alatest` | Supported | |
| `ain_bulk`, `araw`, `aexplain`, `acontains` | Supported | |
| `aaggregate`, `aiterator` | Supported | |
| `filter`, `exclude`, `annotate`, `order_by` | Supported | |
| `select_related`, `prefetch_related` | Supported | |
| `select_for_update` | Supported | |
| `values`, `values_list`, `distinct` | Supported | |
| `dates`, `datetimes`, `alias` | Supported | |
| `union`, `intersection`, `difference` | Supported | |
| `defer`, `only`, `using`, `extra`, `reverse` | Supported | |
| `__aiter__`, `__getitem__` | Supported | |

### Model Instance Methods

| Method | Status | Notes |
|--------|--------|-------|
| `asave()` | Supported | Fires `pre_save`/`post_save` via `asend()` (Django's built-in `asave` uses sync signals) |
| `adelete()` | Supported | CASCADE/SET_NULL/PROTECT/RESTRICT, fires `pre_delete`/`post_delete` via `asend()` |
| `arefresh_from_db()` | Supported | |
| `aget_next_by_FOO()` | Supported | Auto-generated for date/datetime fields |
| `aget_previous_by_FOO()` | Supported | Auto-generated for date/datetime fields |

### Related Manager Methods

Reverse FK and M2M managers on `AsyncModelMixin` subclasses automatically get async methods:

| Method | Reverse FK | M2M | Notes |
|--------|-----------|-----|-------|
| `aadd()` | Yes | Yes | Bulk and non-bulk modes for FK |
| `aremove()` | Yes (nullable FK only) | Yes | |
| `aclear()` | Yes (nullable FK only) | Yes | |
| `aset()` | Yes | Yes | With `clear=True/False` |
| `acreate()` | Yes | Yes | |
| `aget_or_create()` | Yes | Yes | |
| `aupdate_or_create()` | Yes | Yes | |

M2M operations fire `m2m_changed` signals via `asend()`.

---

## Running Tests

```bash
uv sync --group dev
uv run pytest
```

Tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to start a PostgreSQL instance automatically.
