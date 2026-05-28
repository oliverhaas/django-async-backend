import pytest

from django_async_backend.db.models import Model


def test_save_without_asave_raises():
    with pytest.raises(TypeError, match=r"overrides save.*without overriding asave"):

        class BadModel(Model):
            def save(self, *args, **kwargs):
                pass


def test_delete_without_adelete_raises():
    with pytest.raises(TypeError, match=r"overrides delete.*without overriding adelete"):

        class BadModel(Model):
            def delete(self, *args, **kwargs):
                pass


def test_save_and_asave_together_ok():
    class GoodModel(Model):
        def save(self, *args, **kwargs):
            pass

        async def asave(self, *args, **kwargs):
            pass

        class Meta:
            abstract = True


def test_no_overrides_ok():
    class PlainModel(Model):
        class Meta:
            abstract = True


def test_mixin_with_save_only_raises():
    class SaveMixin:
        def save(self, *args, **kwargs):
            pass

    with pytest.raises(TypeError, match="SaveMixin overrides save"):

        class MixedModel(SaveMixin, Model):
            pass


def test_inheriting_compliant_parent_ok():
    class Parent(Model):
        def save(self, *args, **kwargs):
            pass

        async def asave(self, *args, **kwargs):
            pass

        class Meta:
            abstract = True

    class Child(Parent):
        class Meta:
            abstract = True


def test_strict_false_skips_check():
    class Unguarded(Model, async_mro_strict=False):
        def save(self, *args, **kwargs):
            pass

        class Meta:
            abstract = True


def test_strict_false_does_not_propagate_to_children():
    class Parent(Model, async_mro_strict=False):
        def save(self, *args, **kwargs):
            pass

        class Meta:
            abstract = True

    with pytest.raises(TypeError, match="overrides save"):

        class Child(Parent):
            def save(self, *args, **kwargs):
                pass
