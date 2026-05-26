from django_async_backend.db import async_connections as connections

"""
Create SQL statements for QuerySets.

The code in here encapsulates all of the SQL construction so that QuerySets
themselves do not have to (and could be backed by things other than SQL
databases). The abstraction barrier only works one way: this module has to know
all about the internals of models in order to get the information it needs.
"""

import copy
import difflib
import functools
import inspect
import sys
import warnings
from collections import (
    Counter,
    namedtuple,
)
from collections.abc import (
    Iterator,
    Mapping,
)
from itertools import (
    chain,
    count,
    product,
)
from string import ascii_uppercase

from django.core.exceptions import (
    FieldDoesNotExist,
    FieldError,
)
from django.db import (
    DEFAULT_DB_ALIAS,
    NotSupportedError,
)
from django.db.models.aggregates import Count
from django.db.models.sql.query import (
    JoinPromoter,
    Query as DjangoQuery,
    RawQuery as DjangoRawQuery,
)
from django.db.models.constants import LOOKUP_SEP
from django.db.models.expressions import (
    BaseExpression,
    Col,
    ColPairs,
    Exists,
    F,
    OuterRef,
    RawSQL,
    Ref,
    ResolvedOuterRef,
    Value,
)
from django.db.models.fields import Field
from django.db.models.lookups import Lookup
from django.db.models.query_utils import (
    Q,
    check_rel_lookup_compatibility,
    refs_expression,
)
from django.db.models.sql.constants import (
    INNER,
    LOUTER,
    ORDER_DIR,
    SINGLE,
)
from django.db.models.sql.datastructures import (
    BaseTable,
    Empty,
    Join,
    MultiJoin,
)
from django.db.models.sql.where import (
    AND,
    OR,
    ExtraWhere,
    NothingNode,
    WhereNode,
)
from django.utils.deprecation import RemovedInDjango70Warning
from django.utils.functional import cached_property
from django.utils.regex_helper import _lazy_re_compile
from django.utils.tree import Node

__all__ = ["Query", "RawQuery"]

# RemovedInDjango70Warning: When the deprecation ends, replace with:
# Quotation marks ('"`[]), whitespace characters, semicolons, percent signs,
# hashes, or inline SQL comments are forbidden in column aliases.
# FORBIDDEN_ALIAS_PATTERN = _lazy_re_compile(r"['`\"\]\[;\s]|%|#|--|/\*|\*/")
# Quotation marks ('"`[]), whitespace characters, semicolons, hashes, or inline
# SQL comments are forbidden in column aliases.
FORBIDDEN_ALIAS_PATTERN = _lazy_re_compile(r"['`\"\]\[;\s]|#|--|/\*|\*/")

# Inspired from
# https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
EXPLAIN_OPTIONS_PATTERN = _lazy_re_compile(r"[\w-]+")


def get_field_names_from_opts(opts):
    if opts is None:
        return set()
    return set(chain.from_iterable((f.name, f.attname) if f.concrete else (f.name,) for f in opts.get_fields()))


def get_paths_from_expression(expr):
    if isinstance(expr, F):
        yield expr.name
    elif hasattr(expr, "flatten"):
        for child in expr.flatten():
            if isinstance(child, F):
                yield child.name
            elif isinstance(child, Q):
                yield from get_children_from_q(child)


def get_children_from_q(q):
    for child in q.children:
        if isinstance(child, Node):
            yield from get_children_from_q(child)
        elif isinstance(child, tuple):
            lhs, rhs = child
            yield lhs
            if hasattr(rhs, "resolve_expression"):
                yield from get_paths_from_expression(rhs)
        elif hasattr(child, "resolve_expression"):
            yield from get_paths_from_expression(child)


def get_child_with_renamed_prefix(prefix, replacement, child):
    from django.db.models.query import QuerySet

    if isinstance(child, Node):
        return rename_prefix_from_q(prefix, replacement, child)
    if isinstance(child, tuple):
        lhs, rhs = child
        if lhs.startswith(prefix + LOOKUP_SEP):
            lhs = lhs.replace(prefix, replacement, 1)
        if not isinstance(rhs, F) and hasattr(rhs, "resolve_expression"):
            rhs = get_child_with_renamed_prefix(prefix, replacement, rhs)
        return lhs, rhs

    if isinstance(child, F):
        child = child.copy()
        if child.name.startswith(prefix + LOOKUP_SEP):
            child.name = child.name.replace(prefix, replacement, 1)
    elif isinstance(child, QuerySet):
        # QuerySet may contain OuterRef() references which cannot work properly
        # without repointing to the filtered annotation and will spawn a
        # different JOIN. Always raise ValueError instead of providing partial
        # support in other cases.
        raise ValueError("Passing a QuerySet within a FilteredRelation is not supported.")
    elif hasattr(child, "resolve_expression"):
        child = child.copy()
        child.set_source_expressions(
            [
                get_child_with_renamed_prefix(prefix, replacement, grand_child)
                for grand_child in child.get_source_expressions()
            ],
        )
    return child


def rename_prefix_from_q(prefix, replacement, q):
    return Q.create(
        [get_child_with_renamed_prefix(prefix, replacement, c) for c in q.children],
        q.connector,
        q.negated,
    )


JoinInfo = namedtuple(
    "JoinInfo",
    ("final_field", "targets", "opts", "joins", "path", "transform_function"),
)


class RawQuery(DjangoRawQuery):
    """Async-aware RawQuery: inherits sync API from Django, adds async I/O."""

    async def aget_columns(self):
        if self.cursor is None:
            await self._aexecute_query()
        from django.db import connections as django_connections

        converter = django_connections[self.using].introspection.identifier_converter
        return [converter(column_meta[0]) for column_meta in self.cursor.description]

    async def __aiter__(self):
        await self._aexecute_query()
        result = [row async for row in self.cursor]
        for row in result:
            yield row

    async def _aexecute_query(self):
        connection = connections[self.using]

        params_type = self.params_type
        adapter = connection.ops.adapt_unknown_value
        if params_type is tuple:
            params = tuple(adapter(val) for val in self.params)
        elif params_type is dict:
            params = {key: adapter(val) for key, val in self.params.items()}
        elif params_type is None:
            params = None
        else:
            raise RuntimeError("Unexpected params type: %s" % params_type)

        self.cursor = await connection.cursor()
        await self.cursor.execute(self.sql, params)


ExplainInfo = namedtuple("ExplainInfo", ("format", "options"))


class Query(DjangoQuery):
    """A single SQL query.

    Inherits from django.db.models.sql.query.Query (via DjangoQuery alias) so
    isinstance() checks in Django's lookup code (Exact/In get_prep_lookup) pass
    and our sliced/values queryset RHS gets the same column-count validation
    and pk-rewriting that Django's sync path does. Our async overrides
    (get_compiler, get_aggregation, get_count, etc.) take precedence via MRO.
    """

    alias_prefix = "T"
    empty_result_set_value = None
    subq_aliases = frozenset([alias_prefix])

    compiler = "SQLCompiler"

    base_table_class = BaseTable
    join_class = Join

    default_cols = True
    default_ordering = True
    standard_ordering = True

    filter_is_sticky = False
    subquery = False
    contains_subquery = False

    # SQL-related attributes.
    # Select and related select clauses are expressions to use in the SELECT
    # clause of the query. The select is used for cases where we want to set up
    # the select clause to contain other than default fields (values(),
    # subqueries...). Note that annotations go to annotations dictionary.
    select = ()
    # The group_by attribute can have one of the following forms:
    #  - None: no group by at all in the query
    #  - A tuple of expressions: group by (at least) those expressions.
    #    String refs are also allowed for now.
    #  - True: group by all select fields of the model
    # See compiler.get_group_by() for details.
    group_by = None
    order_by = ()
    low_mark = 0  # Used for offset/limit.
    high_mark = None  # Used for offset/limit.
    distinct = False
    distinct_fields = ()
    select_for_update = False
    select_for_update_nowait = False
    select_for_update_skip_locked = False
    select_for_update_of = ()
    select_for_no_key_update = False
    select_related = False
    # Arbitrary limit for select_related to prevents infinite recursion.
    max_depth = 5
    # Holds the selects defined by a call to values() or values_list()
    # excluding annotation_select and extra_select.
    values_select = ()
    selected = None

    # SQL annotation-related attributes.
    annotation_select_mask = None
    _annotation_select_cache = None

    # Set combination attributes.
    combinator = None
    combinator_all = False
    combined_queries = ()

    # These are for extensions. The contents are more or less appended verbatim
    # to the appropriate clause.
    extra_select_mask = None
    _extra_select_cache = None

    extra_tables = ()
    extra_order_by = ()

    # A tuple that is a set of model field names and either True, if these are
    # the fields to defer, or False if these are the only fields to load.
    deferred_loading = (frozenset(), True)

    explain_info = None

    @property
    def output_field(self):
        if len(self.select) == 1:
            select = self.select[0]
            return getattr(select, "target", None) or select.field
        if len(self.annotation_select) == 1:
            return next(iter(self.annotation_select.values())).output_field

    @cached_property
    def base_table(self):
        for alias in self.alias_map:
            return alias

    def get_compiler(self, using=None, connection=None, elide_empty=True):
        if using is None and connection is None:
            raise ValueError("Need either using or connection")
        if using:
            connection = connections[using]
        return connection.ops.compiler(self.compiler)(self, connection, using, elide_empty)

    async def get_aggregation(self, using, aggregate_exprs):
        """
        Return the dictionary with the values of the existing aggregations.
        """
        if not aggregate_exprs:
            return {}
        # Store annotation mask prior to temporarily adding aggregations for
        # resolving purpose to facilitate their subsequent removal.
        refs_subquery = False
        refs_window = False
        replacements = {}
        annotation_select_mask = self.annotation_select_mask
        for alias, aggregate_expr in aggregate_exprs.items():
            self.check_alias(alias)
            aggregate = aggregate_expr.resolve_expression(self, allow_joins=True, reuse=None, summarize=True)
            if not aggregate.contains_aggregate:
                raise TypeError("%s is not an aggregate expression" % alias)
            # Temporarily add aggregate to annotations to allow remaining
            # members of `aggregates` to resolve against each others.
            self.append_annotation_mask([alias])
            aggregate_refs = aggregate.get_refs()
            refs_subquery |= any(getattr(self.annotations[ref], "contains_subquery", False) for ref in aggregate_refs)
            refs_window |= any(getattr(self.annotations[ref], "contains_over_clause", True) for ref in aggregate_refs)
            aggregate = aggregate.replace_expressions(replacements)
            self.annotations[alias] = aggregate
            replacements[Ref(alias, aggregate)] = aggregate
        # Stash resolved aggregates now that they have been allowed to resolve
        # against each other.
        aggregates = {alias: self.annotations.pop(alias) for alias in aggregate_exprs}
        self.set_annotation_mask(annotation_select_mask)
        # Existing usage of aggregation can be determined by the presence of
        # selected aggregates but also by filters against aliased aggregates.
        _, having, qualify = self.where.split_having_qualify()
        has_existing_aggregation = (
            any(getattr(annotation, "contains_aggregate", True) for annotation in self.annotations.values()) or having
        )
        set_returning_annotations = {
            alias for alias, annotation in self.annotation_select.items() if getattr(annotation, "set_returning", False)
        }
        # Decide if we need to use a subquery.
        #
        # Existing aggregations would cause incorrect results as
        # get_aggregation() must produce just one result and thus must not use
        # GROUP BY.
        #
        # If the query has limit or distinct, or uses set operations, then
        # those operations must be done in a subquery so that the query
        # aggregates on the limit and/or distinct results instead of applying
        # the distinct and limit after the aggregation.
        if (
            isinstance(self.group_by, tuple)
            or self.is_sliced
            or has_existing_aggregation
            or refs_subquery
            or refs_window
            or qualify
            or self.distinct
            or self.combinator
            or set_returning_annotations
        ):
            inner_query = self.clone()
            inner_query.subquery = True
            outer_query = AggregateQuery(self.model, inner_query)
            inner_query.select_for_update = False
            inner_query.select_related = False
            inner_query.set_annotation_mask(self.annotation_select)
            # Queries with distinct_fields need ordering and when a limit is
            # applied we must take the slice from the ordered query. Otherwise
            # no need for ordering.
            inner_query.clear_ordering(force=False)
            if not inner_query.distinct:
                # If the inner query uses default select and it has some
                # aggregate annotations, then we must make sure the inner
                # query is grouped by the main model's primary key. However,
                # clearing the select clause can alter results if distinct is
                # used.
                if inner_query.default_cols and has_existing_aggregation:
                    inner_query.group_by = (self.model._meta.pk.get_col(inner_query.get_initial_alias()),)
                inner_query.default_cols = False
                if not qualify and not self.combinator:
                    # Mask existing annotations that are not referenced by
                    # aggregates to be pushed to the outer query unless
                    # filtering against window functions or if the query is
                    # combined as both would require complex realiasing logic.
                    annotation_mask = set()
                    if isinstance(self.group_by, tuple):
                        for expr in self.group_by:
                            annotation_mask |= expr.get_refs()
                    for aggregate in aggregates.values():
                        annotation_mask |= aggregate.get_refs()
                    # Avoid eliding expressions that might have an incidence on
                    # the implicit grouping logic.
                    for (
                        annotation_alias,
                        annotation,
                    ) in self.annotation_select.items():
                        if annotation.get_group_by_cols():
                            annotation_mask.add(annotation_alias)
                    inner_query.set_annotation_mask(annotation_mask)
                    # Annotations that possibly return multiple rows cannot
                    # be masked as they might have an incidence on the query.
                    annotation_mask |= set_returning_annotations

            # Add aggregates to the outer AggregateQuery. This requires making
            # sure all columns referenced by the aggregates are selected in the
            # inner query. It is achieved by retrieving all column references
            # by the aggregates, explicitly selecting them in the inner query,
            # and making sure the aggregates are repointed to them.
            col_refs = {}
            for alias, aggregate in aggregates.items():
                replacements = {}
                for col in self._gen_cols([aggregate], resolve_refs=False):
                    if not (col_ref := col_refs.get(col)):
                        index = len(col_refs) + 1
                        col_alias = f"__col{index}"
                        col_ref = Ref(col_alias, col)
                        col_refs[col] = col_ref
                        inner_query.add_annotation(col, col_alias)
                    replacements[col] = col_ref
                outer_query.annotations[alias] = aggregate.replace_expressions(replacements)
            if inner_query.select == () and not inner_query.default_cols and not inner_query.annotation_select_mask:
                # In case of Model.objects[0:3].count(), there would be no
                # field selected in the inner query, yet we must use a
                # subquery. So, make sure at least one field is selected.
                inner_query.select = (self.model._meta.pk.get_col(inner_query.get_initial_alias()),)
        else:
            outer_query = self
            self.select = ()
            self.selected = None
            self.default_cols = False
            self.extra = {}
            if self.annotations:
                # Inline reference to existing annotations and mask them as
                # they are unnecessary given only the summarized aggregations
                # are requested.
                replacements = {Ref(alias, annotation): annotation for alias, annotation in self.annotations.items()}
                self.annotations = {
                    alias: aggregate.replace_expressions(replacements) for alias, aggregate in aggregates.items()
                }
            else:
                self.annotations = aggregates
            self.set_annotation_mask(aggregates)

        empty_set_result = [expression.empty_result_set_value for expression in outer_query.annotation_select.values()]
        elide_empty = not any(result is NotImplemented for result in empty_set_result)
        outer_query.clear_ordering(force=True)
        outer_query.clear_limits()
        outer_query.select_for_update = False
        outer_query.select_related = False
        compiler = outer_query.get_compiler(using, elide_empty=elide_empty)
        result = await compiler.aexecute_sql(SINGLE)
        if result is None:
            result = empty_set_result
        else:
            cols = outer_query.annotation_select.values()
            converters = compiler.get_converters(cols)
            rows = compiler.apply_converters((result,), converters)
            if compiler.has_composite_fields(cols):
                rows = compiler.composite_fields_to_tuples(rows, cols)
            result = next(rows)

        return dict(zip(outer_query.annotation_select, result))

    async def get_count(self, using):
        """
        Perform a COUNT() query using the current filter constraints.
        """
        obj = self.clone()
        return (await obj.get_aggregation(using, {"__count": Count("*")}))["__count"]

    async def ahas_results(self, using):
        q = self.exists()
        compiler = q.get_compiler(using=using)
        return await compiler.ahas_results()

    async def explain(self, using, format=None, **options):
        q = self.clone()
        for option_name in options:
            if not EXPLAIN_OPTIONS_PATTERN.fullmatch(option_name) or "--" in option_name:
                raise ValueError(f"Invalid option name: {option_name!r}.")
        q.explain_info = ExplainInfo(format, options)
        compiler = q.get_compiler(using=using)
        return "\n".join([i async for i in compiler.aexplain_query()])

    def check_alias(self, alias):
        # RemovedInDjango70Warning: When the deprecation ends, remove.
        if "%" in alias:
            if "aggregate" in {frame.function for frame in inspect.stack()}:
                stacklevel = 5
            else:
                # annotate(), alias(), and values().
                stacklevel = 6
            warnings.warn(
                "Using percent signs in a column alias is deprecated.",
                stacklevel=stacklevel,
                category=RemovedInDjango70Warning,
            )
        if FORBIDDEN_ALIAS_PATTERN.search(alias):
            raise ValueError(
                "Column aliases cannot contain whitespace characters, hashes, "
                # RemovedInDjango70Warning: When the deprecation ends, replace
                # with:
                # "quotation marks, semicolons, percent signs, or SQL "
                # "comments."
                "quotation marks, semicolons, or SQL comments.",
            )

    @property
    def _subquery_fields_len(self):
        if self.has_select_fields:
            return sum(len(self.model._meta.pk_fields) if field == "pk" else 1 for field in self.selected)
        return len(self.model._meta.pk_fields)

    def check_related_objects(self, field, value, opts):
        """Check the type of object passed to query relations."""
        if field.is_relation:
            # Check that the field and the queryset use the same model in a
            # query like .filter(author=Author.objects.all()). For example, the
            # opts would be Author's (from the author field) and value.model
            # would be Author.objects.all() queryset's .model (Author also).
            # The field is the related field on the lhs side.
            if (
                isinstance(value, Query)
                and not value.has_select_fields
                and not check_rel_lookup_compatibility(value.model, opts, field)
            ):
                raise ValueError(
                    'Cannot use QuerySet for "%s": Use a QuerySet for "%s".'
                    % (value.model._meta.object_name, opts.object_name),
                )
            if hasattr(value, "_meta"):
                self.check_query_object_type(value, opts, field)
            elif hasattr(value, "__iter__"):
                for v in value:
                    self.check_query_object_type(v, opts, field)

    def build_lookup(self, lookups, lhs, rhs):
        """
        Try to extract transforms and lookup from given lhs.

        The lhs value is something that works like SQLExpression.
        The rhs value is what the lookup is going to compare against.
        The lookups is a list of names to extract using get_lookup()
        and get_transform().
        """
        # __exact is the default lookup if one isn't given.
        *transforms, lookup_name = lookups or ["exact"]
        for name in transforms:
            lhs = self.try_transform(lhs, name, lookups)
        # First try get_lookup() so that the lookup takes precedence if the lhs
        # supports both transform and lookup for the name.
        lookup_class = lhs.get_lookup(lookup_name)
        if not lookup_class:
            # A lookup wasn't found. Try to interpret the name as a transform
            # and do an Exact lookup against it.
            lhs = self.try_transform(lhs, lookup_name)
            lookup_name = "exact"
            lookup_class = lhs.get_lookup(lookup_name)
            if not lookup_class:
                return None

        lookup = lookup_class(lhs, rhs)
        # Interpret '__exact=None' as the sql 'is NULL'; otherwise, reject all
        # uses of None as a query value unless the lookup supports it.
        if lookup.rhs is None and not lookup.can_use_none_as_rhs:
            if lookup_name not in ("exact", "iexact"):
                raise ValueError("Cannot use None as a query value")
            return lhs.get_lookup("isnull")(lhs, True)

        # For Oracle '' is equivalent to null. The check must be done at this
        # stage because join promotion can't be done in the compiler. Using
        # DEFAULT_DB_ALIAS isn't nice but it's the best that can be done here.
        # A similar thing is done in is_nullable(), too.
        if (
            lookup_name == "exact"
            and lookup.rhs == ""
            and connections[DEFAULT_DB_ALIAS].features.interprets_empty_strings_as_nulls
        ):
            return lhs.get_lookup("isnull")(lhs, True)

        return lookup

    def try_transform(self, lhs, name, lookups=None):
        """
        Helper method for build_lookup(). Try to fetch and initialize
        a transform for name parameter from lhs.
        """
        transform_class = lhs.get_transform(name)
        if transform_class:
            return transform_class(lhs)
        output_field = lhs.output_field.__class__
        suggested_lookups = difflib.get_close_matches(name, lhs.output_field.get_lookups())
        if suggested_lookups:
            suggestion = ", perhaps you meant %s?" % " or ".join(suggested_lookups)
        else:
            suggestion = "."
        if lookups is not None:
            name_index = lookups.index(name)
            unsupported_lookup = LOOKUP_SEP.join(lookups[name_index:])
        else:
            unsupported_lookup = name
        raise FieldError(
            "Unsupported lookup '%s' for %s or join on the field not "
            "permitted%s" % (unsupported_lookup, output_field.__name__, suggestion),
        )

    def add_filtered_relation(self, filtered_relation, alias):
        self.check_alias(alias)
        filtered_relation.alias = alias
        relation_lookup_parts, relation_field_parts, _ = self.solve_lookup_type(filtered_relation.relation_name)
        if relation_lookup_parts:
            raise ValueError(
                "FilteredRelation's relation_name cannot contain lookups (got %r)." % filtered_relation.relation_name,
            )
        for lookup in get_children_from_q(filtered_relation.condition):
            lookup_parts, lookup_field_parts, _ = self.solve_lookup_type(lookup)
            shift = 2 if not lookup_parts else 1
            lookup_field_path = lookup_field_parts[:-shift]
            for idx, lookup_field_part in enumerate(lookup_field_path):
                if len(relation_field_parts) > idx:
                    if relation_field_parts[idx] != lookup_field_part:
                        raise ValueError(
                            "FilteredRelation's condition doesn't support "
                            "relations outside the %r (got %r)." % (filtered_relation.relation_name, lookup),
                        )
            if len(lookup_field_parts) > len(relation_field_parts) + 1:
                raise ValueError(
                    "FilteredRelation's condition doesn't support nested "
                    "relations deeper than the relation_name (got %r for "
                    "%r)." % (lookup, filtered_relation.relation_name),
                )
        filtered_relation = filtered_relation.clone()
        filtered_relation.condition = rename_prefix_from_q(
            filtered_relation.relation_name,
            alias,
            filtered_relation.condition,
        )
        self._filtered_relations[filtered_relation.alias] = filtered_relation

    def setup_joins(
        self,
        names,
        opts,
        alias,
        can_reuse=None,
        allow_many=True,
    ):
        """
        Compute the necessary table joins for the passage through the fields
        given in 'names'. 'opts' is the Options class for the current model
        (which gives the table we are starting from), 'alias' is the alias for
        the table to start the joining from.

        The 'can_reuse' defines the reverse foreign key joins we can reuse. It
        can be None in which case all joins are reusable or a set of aliases
        that can be reused. Note that non-reverse foreign keys are always
        reusable when using setup_joins().

        If 'allow_many' is False, then any reverse foreign key seen will
        generate a MultiJoin exception.

        Return the final field involved in the joins, the target field (used
        for any 'where' constraint), the final 'opts' value, the joins, the
        field path traveled to generate the joins, and a transform function
        that takes a field and alias and is equivalent to
        `field.get_col(alias)` in the simple case but wraps field transforms if
        they were included in names.

        The target field is the field containing the concrete value. Final
        field can be something different, for example foreign key pointing to
        that value. Final field is needed for example in some value
        conversions (convert 'obj' in fk__id=obj to pk val using the foreign
        key field for example).
        """
        joins = [alias]
        # The transform can't be applied yet, as joins must be trimmed later.
        # To avoid making every caller of this method look up transforms
        # directly, compute transforms here and create a partial that converts
        # fields to the appropriate wrapped version.

        def final_transformer(field, alias):
            if not self.alias_cols:
                alias = None
            return field.get_col(alias)

        # Try resolving all the names as fields first. If there's an error,
        # treat trailing names as lookups until a field can be resolved.
        last_field_exception = None
        for pivot in range(len(names), 0, -1):
            try:
                path, final_field, targets, rest = self.names_to_path(
                    names[:pivot],
                    opts,
                    allow_many,
                    fail_on_missing=True,
                )
            except FieldError as exc:
                if pivot == 1:
                    # The first item cannot be a lookup, so it's safe
                    # to raise the field error here.
                    raise
                last_field_exception = exc
            else:
                # The transforms are the remaining items that couldn't be
                # resolved into fields.
                transforms = names[pivot:]
                break
        for name in transforms:

            def transform(field, alias, *, name, previous):
                try:
                    wrapped = previous(field, alias)
                    return self.try_transform(wrapped, name)
                except FieldError:
                    # FieldError is raised if the transform doesn't exist.
                    if isinstance(final_field, Field) and last_field_exception:
                        raise last_field_exception
                    raise

            final_transformer = functools.partial(transform, name=name, previous=final_transformer)
            final_transformer.has_transforms = True
        # Then, add the path to the query's joins. Note that we can't trim
        # joins at this stage - we will need the information about join type
        # of the trimmed joins.
        for join in path:
            if join.filtered_relation:
                filtered_relation = join.filtered_relation.clone()
                table_alias = filtered_relation.alias
            else:
                filtered_relation = None
                table_alias = None
            opts = join.to_opts
            if join.direct:
                nullable = self.is_nullable(join.join_field)
            else:
                nullable = True
            connection = self.join_class(
                opts.db_table,
                alias,
                table_alias,
                INNER,
                join.join_field,
                nullable,
                filtered_relation=filtered_relation,
            )
            reuse = can_reuse if join.m2m else None
            alias = self.join(connection, reuse=reuse)
            joins.append(alias)
            if join.filtered_relation and can_reuse is not None:
                can_reuse.add(alias)
        return JoinInfo(final_field, targets, opts, joins, path, final_transformer)

    @classmethod
    def _gen_cols(cls, exprs, include_external=False, resolve_refs=True):
        for expr in exprs:
            if isinstance(expr, Col):
                yield expr
            elif include_external and callable(getattr(expr, "get_external_cols", None)):
                yield from expr.get_external_cols()
            elif hasattr(expr, "get_source_expressions"):
                if not resolve_refs and isinstance(expr, Ref):
                    continue
                yield from cls._gen_cols(
                    expr.get_source_expressions(),
                    include_external=include_external,
                    resolve_refs=resolve_refs,
                )

    @classmethod
    def _gen_col_aliases(cls, exprs):
        yield from (expr.alias for expr in cls._gen_cols(exprs))

    def resolve_ref(self, name, allow_joins=True, reuse=None, summarize=False):
        annotation = self.annotations.get(name)
        if annotation is not None:
            if not allow_joins:
                for alias in self._gen_col_aliases([annotation]):
                    if isinstance(self.alias_map[alias], Join):
                        raise FieldError("Joined field references are not permitted in this query")
            if summarize:
                # Summarize currently means we are doing an aggregate() query
                # which is executed as a wrapped subquery if any of the
                # aggregate() elements reference an existing annotation. In
                # that case we need to return a Ref to the subquery's
                # annotation.
                if name not in self.annotation_select:
                    raise FieldError("Cannot aggregate over the '%s' alias. Use annotate() to promote it." % name)
                return Ref(name, self.annotation_select[name])
            return annotation
        field_list = name.split(LOOKUP_SEP)
        annotation = self.annotations.get(field_list[0])
        if annotation is not None:
            for transform in field_list[1:]:
                annotation = self.try_transform(annotation, transform)
            return annotation
        join_info = self.setup_joins(
            field_list,
            self.get_meta(),
            self.get_initial_alias(),
            can_reuse=reuse,
        )
        targets, final_alias, join_list = self.trim_joins(join_info.targets, join_info.joins, join_info.path)
        if not allow_joins and len(join_list) > 1:
            raise FieldError("Joined field references are not permitted in this query")
        if len(targets) > 1:
            raise FieldError("Referencing multicolumn fields with F() objects isn't supported")
        # Verify that the last lookup in name is a field or a transform:
        # transform_function() raises FieldError if not.
        transform = join_info.transform_function(targets[0], final_alias)
        if reuse is not None:
            reuse.update(join_list)
        return transform

    @property
    def is_sliced(self):
        return self.low_mark != 0 or self.high_mark is not None

    def add_fields(self, field_names, allow_m2m=True):
        """
        Add the given (model) fields to the select set. Add the field names in
        the order specified.
        """
        alias = self.get_initial_alias()
        opts = self.get_meta()

        try:
            cols = []
            for name in field_names:
                # Join promotion note - we must not remove any rows here, so
                # if there is no existing joins, use outer join.
                join_info = self.setup_joins(name.split(LOOKUP_SEP), opts, alias, allow_many=allow_m2m)
                targets, final_alias, joins = self.trim_joins(
                    join_info.targets,
                    join_info.joins,
                    join_info.path,
                )
                if len(targets) > 1:
                    transformed_targets = [join_info.transform_function(target, final_alias) for target in targets]
                    cols.append(
                        ColPairs(
                            final_alias if self.alias_cols else None,
                            [col.target for col in transformed_targets],
                            [col.output_field for col in transformed_targets],
                            join_info.final_field,
                        ),
                    )
                else:
                    cols.append(join_info.transform_function(targets[0], final_alias))
            if cols:
                self.set_select(cols)
        except MultiJoin:
            raise FieldError("Invalid field name: '%s'" % name)
        except FieldError:
            if LOOKUP_SEP in name:
                # For lookups spanning over relationships, show the error
                # from the model on which the lookup failed.
                raise
            names = sorted(
                [
                    *get_field_names_from_opts(opts),
                    *self.extra,
                    *self.annotation_select,
                    *self._filtered_relations,
                ],
            )
            raise FieldError("Cannot resolve keyword %r into field. Choices are: %s" % (name, ", ".join(names)))

    @property
    def has_select_fields(self):
        return self.selected is not None

    @property
    def annotation_select(self):
        """
        Return the dictionary of aggregate columns that are not masked and
        should be used in the SELECT clause. Cache this result for performance.
        """
        if self._annotation_select_cache is not None:
            return self._annotation_select_cache
        if not self.annotations:
            return {}
        if self.annotation_select_mask is not None:
            self._annotation_select_cache = {
                k: v for k, v in self.annotations.items() if k in self.annotation_select_mask
            }
            return self._annotation_select_cache
        return self.annotations

    @property
    def extra_select(self):
        if self._extra_select_cache is not None:
            return self._extra_select_cache
        if not self.extra:
            return {}
        if self.extra_select_mask is not None:
            self._extra_select_cache = {k: v for k, v in self.extra.items() if k in self.extra_select_mask}
            return self._extra_select_cache
        return self.extra

    def is_nullable(self, field):
        """
        Check if the given field should be treated as nullable.

        Some backends treat '' as null and Django treats such fields as
        nullable for those backends. In such situations field.null can be
        False even if we should treat the field as nullable.
        """
        # We need to use DEFAULT_DB_ALIAS here, as QuerySet does not have
        # (nor should it have) knowledge of which connection is going to be
        # used. The proper fix would be to defer all decisions where
        # is_nullable() is needed to the compiler stage, but that is not easy
        # to do currently.
        return field.null or (
            field.empty_strings_allowed and connections[DEFAULT_DB_ALIAS].features.interprets_empty_strings_as_nulls
        )


def get_order_dir(field, default="ASC"):
    """
    Return the field name and direction for an order specification. For
    example, '-foo' is returned as ('foo', 'DESC').

    The 'default' param is used to indicate which way no prefix (or a '+'
    prefix) should sort. The '-' prefix always sorts the opposite way.
    """
    dirn = ORDER_DIR[default]
    if field[0] == "-":
        return field[1:], dirn[1]
    return field, dirn[0]


class AggregateQuery(Query):
    """
    Async-aware counterpart to django.db.models.sql.subqueries.AggregateQuery.
    Inheriting from our Query ensures get_compiler() returns our async compiler.
    """

    compiler = "SQLAggregateCompiler"

    def __init__(self, model, inner_query):
        self.inner_query = inner_query
        super().__init__(model)


# JoinPromoter is imported from django.db.models.sql.query at the top.
# We have no async-specific changes to it.
