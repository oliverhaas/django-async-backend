import collections
import json
import re
from functools import partial
from itertools import chain

from django.core.exceptions import (
    EmptyResultSet,
    FieldError,
    FullResultSet,
)
from django.db import (
    DatabaseError,
    NotSupportedError,
)
from django.db.models.constants import LOOKUP_SEP
from django.db.models.expressions import (
    ColPairs,
    F,
    OrderBy,
    RawSQL,
    Ref,
    Value,
)
from django.db.models.fields import (
    AutoField,
    composite,
)
from django.db.models.functions import (
    Cast,
    Random,
)
from django.db.models.lookups import Lookup
from django.db.models.query_utils import select_related_descend
from django.db.models.sql.constants import (
    CURSOR,
    GET_ITERATOR_CHUNK_SIZE,
    MULTI,
    NO_RESULTS,
    ORDER_DIR,
    ROW_COUNT,
    SINGLE,
)
from django.db.models.sql.compiler import (
    SQLAggregateCompiler as DjangoSQLAggregateCompiler,
    SQLCompiler as DjangoSQLCompiler,
    SQLDeleteCompiler as DjangoSQLDeleteCompiler,
    SQLInsertCompiler as DjangoSQLInsertCompiler,
    SQLUpdateCompiler as DjangoSQLUpdateCompiler,
)
from django.db.models.sql.query import (
    Query,
    get_order_dir,
)
from django.db.transaction import TransactionManagementError
from django.utils.functional import cached_property
from django.utils.hashable import make_hashable
from django.utils.regex_helper import _lazy_re_compile


class PositionRef(Ref):
    def __init__(self, ordinal, refs, source):
        self.ordinal = ordinal
        super().__init__(refs, source)

    def as_sql(self, compiler, connection):
        return str(self.ordinal), ()


class SQLCompiler(DjangoSQLCompiler):
    # Multiline ordering SQL clause may appear from RawSQL.
    ordering_parts = _lazy_re_compile(
        r"^(.*)\s(?:ASC|DESC).*",
        re.MULTILINE | re.DOTALL,
    )

    def _order_by_pairs(self):
        if self.query.extra_order_by:
            ordering = self.query.extra_order_by
        elif not self.query.default_ordering or self.query.order_by:
            ordering = self.query.order_by
        elif (meta := self.query.get_meta()) and meta.ordering:
            ordering = meta.ordering
            self._meta_ordering = ordering
        else:
            ordering = []
        if self.query.standard_ordering:
            default_order, _ = ORDER_DIR["ASC"]
        else:
            default_order, _ = ORDER_DIR["DESC"]

        selected_exprs = {}
        # Avoid computing `selected_exprs` if there is no `ordering` as it's
        # relatively expensive.
        if ordering and (select := self.select):
            for ordinal, (expr, _, alias) in enumerate(select, start=1):
                pos_expr = PositionRef(ordinal, alias, expr)
                if alias:
                    selected_exprs[alias] = pos_expr
                selected_exprs[expr] = pos_expr

        for field in ordering:
            if hasattr(field, "resolve_expression"):
                if isinstance(field, Value):
                    # output_field must be resolved for constants.
                    field = Cast(field, field.output_field)
                if not isinstance(field, OrderBy):
                    field = field.asc()
                if not self.query.standard_ordering:
                    field = field.copy()
                    field.reverse_ordering()
                select_ref = selected_exprs.get(field.expression)
                if select_ref or (
                    isinstance(field.expression, F) and (select_ref := selected_exprs.get(field.expression.name))
                ):
                    # Emulation of NULLS (FIRST|LAST) cannot be combined with
                    # the usage of ordering by position.
                    if (
                        field.nulls_first is None and field.nulls_last is None
                    ) or self.connection.features.supports_order_by_nulls_modifier:
                        field = field.copy()
                        field.expression = select_ref
                    # Alias collisions are not possible when dealing with
                    # combined queries so fallback to it if emulation of NULLS
                    # handling is required.
                    elif self.query.combinator:
                        field = field.copy()
                        field.expression = Ref(select_ref.refs, select_ref.source)
                yield field, select_ref is not None
                continue
            if field == "?":  # random
                yield OrderBy(Random()), False
                continue

            col, order = get_order_dir(field, default_order)
            descending = order == "DESC"

            if select_ref := selected_exprs.get(col):
                # Reference to expression in SELECT clause
                yield (
                    OrderBy(
                        select_ref,
                        descending=descending,
                    ),
                    True,
                )
                continue

            if expr := self.query.annotations.get(col):
                ref = col
                transforms = []
            else:
                ref, *transforms = col.split(LOOKUP_SEP)
                expr = self.query.annotations.get(ref)
            if expr:
                if self.query.combinator and self.select:
                    if transforms:
                        raise NotImplementedError("Ordering combined queries by transforms is not implemented.")
                    # Don't use the resolved annotation because other
                    # combined queries might define it differently.
                    expr = F(ref)
                if transforms:
                    for name in transforms:
                        expr = self.query.try_transform(expr, name)
                if isinstance(expr, Value):
                    # output_field must be resolved for constants.
                    expr = Cast(expr, expr.output_field)
                yield OrderBy(expr, descending=descending), False
                continue

            if "." in field:
                # This came in through an extra(order_by=...) addition. Pass it
                # on verbatim.
                table, col = col.split(".", 1)
                yield (
                    OrderBy(
                        RawSQL(
                            "%s.%s" % (self.quote_name_unless_alias(table), col),
                            [],
                        ),
                        descending=descending,
                    ),
                    False,
                )
                continue

            if self.query.extra and col in self.query.extra:
                if col in self.query.extra_select:
                    yield (
                        OrderBy(
                            Ref(col, RawSQL(*self.query.extra[col])),
                            descending=descending,
                        ),
                        True,
                    )
                else:
                    yield (
                        OrderBy(
                            RawSQL(*self.query.extra[col]),
                            descending=descending,
                        ),
                        False,
                    )
            elif self.query.combinator and self.select:
                # Don't use the first model's field because other
                # combinated queries might define it differently.
                yield OrderBy(F(col), descending=descending), False
            else:
                # 'col' is of the form 'field' or 'field1__field2' or
                # '-field1__field2__field', etc.
                yield from self.find_ordering_name(
                    field,
                    self.query.get_meta(),
                    default_order=default_order,
                )

    def get_order_by(self):
        """
        Return a list of 2-tuples of the form (expr, (sql, params, is_ref)) for
        the ORDER BY clause.

        The order_by clause can alter the select clause (for example it can add
        aliases to clauses that do not yet have one, or it can add totally new
        select clauses).
        """
        result = []
        seen = set()
        for expr, is_ref in self._order_by_pairs():
            resolved = expr.resolve_expression(self.query, allow_joins=True, reuse=None)
            if not is_ref and self.query.combinator and self.select:
                src = resolved.expression
                expr_src = expr.expression
                for sel_expr, _, col_alias in self.select:
                    if src == sel_expr:
                        # When values() is used the exact alias must be used to
                        # reference annotations.
                        if (
                            self.query.has_select_fields
                            and col_alias in self.query.annotation_select
                            and not (isinstance(expr_src, F) and col_alias == expr_src.name)
                        ):
                            continue
                        resolved.set_source_expressions(
                            [
                                Ref(
                                    (col_alias or src.target.column),
                                    src,
                                ),
                            ],
                        )
                        break
                else:
                    # Add column used in ORDER BY clause to the selected
                    # columns and to each combined query.
                    order_by_idx = len(self.query.select) + 1
                    col_alias = f"__orderbycol{order_by_idx}"
                    for q in self.query.combined_queries:
                        # If fields were explicitly selected through values()
                        # combined queries cannot be augmented.
                        if q.has_select_fields:
                            raise DatabaseError("ORDER BY term does not match any column in the result set.")
                        q.add_annotation(expr_src, col_alias)
                    self.query.add_select_col(resolved, col_alias)
                    resolved.set_source_expressions([Ref(col_alias, src)])
            sql, params = self.compile(resolved)
            # Don't add the same column twice, but the order direction is
            # not taken into account so we strip it. When this entire method
            # is refactored into expressions, then we can check each part as we
            # generate it.
            without_ordering = self.ordering_parts.search(sql)[1]
            params_hash = make_hashable(params)
            if (without_ordering, params_hash) in seen:
                continue
            seen.add((without_ordering, params_hash))
            result.append((resolved, (sql, params, is_ref)))
        return result

    def get_combinator_sql(self, combinator, all):
        features = self.connection.features
        compilers = [
            query.get_compiler(self.using, self.connection, self.elide_empty) for query in self.query.combined_queries
        ]
        if not features.supports_slicing_ordering_in_compound:
            for compiler in compilers:
                if compiler.query.is_sliced:
                    raise DatabaseError("LIMIT/OFFSET not allowed in subqueries of compound statements.")
                if compiler.get_order_by():
                    raise DatabaseError("ORDER BY not allowed in subqueries of compound statements.")
        parts = []
        empty_compiler = None
        for compiler in compilers:
            try:
                parts.append(self._get_combinator_part_sql(compiler))
            except EmptyResultSet:
                # Omit the empty queryset with UNION and with DIFFERENCE if the
                # first queryset is nonempty.
                if combinator == "union" or (combinator == "difference" and parts):
                    empty_compiler = compiler
                    continue
                raise
        if not parts:
            raise EmptyResultSet
        if len(parts) == 1 and combinator == "union" and self.query.is_sliced:
            # A sliced union cannot be composed of a single component because
            # in the event the later is also sliced it might result in invalid
            # SQL due to the usage of multiple LIMIT clauses. Prevent that from
            # happening by always including an empty resultset query to force
            # the creation of an union.
            empty_compiler.elide_empty = False
            parts.append(self._get_combinator_part_sql(empty_compiler))
        combinator_sql = self.connection.ops.set_operators[combinator]
        if all and combinator == "union":
            combinator_sql += " ALL"
        braces = "{}"
        if not self.query.subquery and features.supports_slicing_ordering_in_compound:
            braces = "({})"
        sql_parts, args_parts = zip(*((braces.format(sql), args) for sql, args in parts))
        result = [f" {combinator_sql} ".join(sql_parts)]
        params = []
        for part in args_parts:
            params.extend(part)
        return result, params

    def _get_combinator_part_sql(self, compiler):
        features = self.connection.features
        # If the columns list is limited, then all combined queries
        # must have the same columns list. Set the selects defined on
        # the query on all combined queries, if not already set.
        selected = self.query.selected
        if selected is not None and compiler.query.selected is None:
            compiler.query = compiler.query.clone()
            compiler.query.set_values(selected)
        part_sql, part_args = compiler.as_sql(with_col_aliases=True)
        if compiler.query.combinator:
            # Wrap in a subquery if wrapping in parentheses isn't
            # supported.
            if not features.supports_parentheses_in_compound:
                part_sql = f"SELECT * FROM ({part_sql})"
            # Add parentheses when combining with compound query if not
            # already added for all compound queries.
            elif self.query.subquery or not features.supports_slicing_ordering_in_compound:
                part_sql = f"({part_sql})"
        elif self.query.subquery and features.supports_slicing_ordering_in_compound:
            part_sql = f"({part_sql})"
        return part_sql, part_args

    def get_qualify_sql(self):
        where_parts = []
        if self.where:
            where_parts.append(self.where)
        if self.having:
            where_parts.append(self.having)
        inner_query = self.query.clone()
        inner_query.subquery = True
        inner_query.where = inner_query.where.__class__(where_parts)
        # Augment the inner query with any window function references that
        # might have been masked via values() and alias(). If any masked
        # aliases are added they'll be masked again to avoid fetching
        # the data in the `if qual_aliases` branch below.
        select = {expr: alias for expr, _, alias in self.get_select(with_col_aliases=True)[0]}
        select_aliases = set(select.values())
        qual_aliases = set()
        replacements = {}

        def collect_replacements(expressions):
            while expressions:
                expr = expressions.pop()
                if expr in replacements:
                    continue
                if select_alias := select.get(expr):
                    replacements[expr] = select_alias
                elif isinstance(expr, Lookup):
                    expressions.extend(expr.get_source_expressions())
                elif isinstance(expr, Ref):
                    if expr.refs not in select_aliases:
                        expressions.extend(expr.get_source_expressions())
                else:
                    num_qual_alias = len(qual_aliases)
                    select_alias = f"qual{num_qual_alias}"
                    qual_aliases.add(select_alias)
                    inner_query.add_annotation(expr, select_alias)
                    replacements[expr] = select_alias

        collect_replacements(list(self.qualify.leaves()))
        self.qualify = self.qualify.replace_expressions(
            {expr: Ref(alias, expr) for expr, alias in replacements.items()},
        )
        order_by = []
        for order_by_expr, *_ in self.get_order_by():
            collect_replacements(order_by_expr.get_source_expressions())
            order_by.append(
                order_by_expr.replace_expressions({expr: Ref(alias, expr) for expr, alias in replacements.items()}),
            )
        inner_query_compiler = inner_query.get_compiler(
            self.using,
            connection=self.connection,
            elide_empty=self.elide_empty,
        )
        inner_sql, inner_params = inner_query_compiler.as_sql(
            # The limits must be applied to the outer query to avoid pruning
            # results too eagerly.
            with_limits=False,
            # Force unique aliasing of selected columns to avoid collisions
            # and make rhs predicates referencing easier.
            with_col_aliases=True,
        )
        qualify_sql, qualify_params = self.compile(self.qualify)
        result = [
            "SELECT * FROM (",
            inner_sql,
            ")",
            self.connection.ops.quote_name("qualify"),
            "WHERE",
            qualify_sql,
        ]
        if qual_aliases:
            # If some select aliases were unmasked for filtering purposes they
            # must be masked back.
            cols = [self.connection.ops.quote_name(alias) for alias in select.values()]
            result = [
                "SELECT",
                ", ".join(cols),
                "FROM (",
                *result,
                ")",
                self.connection.ops.quote_name("qualify_mask"),
            ]
        params = list(inner_params) + qualify_params
        # As the SQL spec is unclear on whether or not derived tables
        # ordering must propagate it has to be explicitly repeated on the
        # outer-most query to ensure it's preserved.
        if order_by:
            ordering_sqls = []
            for ordering in order_by:
                ordering_sql, ordering_params = self.compile(ordering)
                ordering_sqls.append(ordering_sql)
                params.extend(ordering_params)
            result.extend(["ORDER BY", ", ".join(ordering_sqls)])
        return result, params

    def as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Create the SQL for this query. Return the SQL string and list of
        parameters.

        If 'with_limits' is False, any limit/offset information is not included
        in the query.
        """
        refcounts_before = self.query.alias_refcount.copy()
        try:
            combinator = self.query.combinator
            extra_select, order_by, group_by = self.pre_sql_setup(
                with_col_aliases=with_col_aliases or bool(combinator),
            )
            for_update_part = None
            # Is a LIMIT/OFFSET clause needed?
            with_limit_offset = with_limits and self.query.is_sliced
            combinator = self.query.combinator
            features = self.connection.features
            if combinator:
                if not getattr(features, f"supports_select_{combinator}"):
                    raise NotSupportedError(f"{combinator} is not supported on this database backend.")
                result, params = self.get_combinator_sql(combinator, self.query.combinator_all)
            elif self.qualify:
                result, params = self.get_qualify_sql()
                order_by = None
            else:
                distinct_fields, distinct_params = self.get_distinct()
                # This must come after 'select', 'ordering', and 'distinct'
                # (see docstring of get_from_clause() for details).
                from_, f_params = self.get_from_clause()
                try:
                    where, w_params = self.compile(self.where) if self.where is not None else ("", [])
                except EmptyResultSet:
                    if self.elide_empty:
                        raise
                    # Use a predicate that's always False.
                    where, w_params = "0 = 1", []
                except FullResultSet:
                    where, w_params = "", []
                try:
                    having, h_params = self.compile(self.having) if self.having is not None else ("", [])
                except FullResultSet:
                    having, h_params = "", []
                result = ["SELECT"]
                params = []

                if self.query.distinct:
                    distinct_result, distinct_params = self.connection.ops.distinct_sql(
                        distinct_fields,
                        distinct_params,
                    )
                    result += distinct_result
                    params += distinct_params

                out_cols = []
                for _, (s_sql, s_params), alias in self.select + extra_select:
                    if alias:
                        s_sql = "%s AS %s" % (
                            s_sql,
                            self.connection.ops.quote_name(alias),
                        )
                    params.extend(s_params)
                    out_cols.append(s_sql)

                result += [", ".join(out_cols)]
                if from_:
                    result += ["FROM", *from_]
                elif self.connection.features.bare_select_suffix:
                    result += [self.connection.features.bare_select_suffix]
                params.extend(f_params)

                if self.query.select_for_update and features.has_select_for_update:
                    if (
                        self.connection.autocommit
                        # Don't raise an exception when database doesn't
                        # support transactions, as it's a noop.
                        and features.supports_transactions
                    ):
                        raise TransactionManagementError("select_for_update cannot be used outside of a transaction.")

                    if with_limit_offset and not features.supports_select_for_update_with_limit:
                        raise NotSupportedError(
                            "LIMIT/OFFSET is not supported with select_for_update on this database backend.",
                        )
                    nowait = self.query.select_for_update_nowait
                    skip_locked = self.query.select_for_update_skip_locked
                    of = self.query.select_for_update_of
                    no_key = self.query.select_for_no_key_update
                    # If it's a NOWAIT/SKIP LOCKED/OF/NO KEY query but the
                    # backend doesn't support it, raise NotSupportedError to
                    # prevent a possible deadlock.
                    if nowait and not features.has_select_for_update_nowait:
                        raise NotSupportedError("NOWAIT is not supported on this database backend.")
                    if skip_locked and not features.has_select_for_update_skip_locked:
                        raise NotSupportedError("SKIP LOCKED is not supported on this database backend.")
                    if of and not features.has_select_for_update_of:
                        raise NotSupportedError("FOR UPDATE OF is not supported on this database backend.")
                    if no_key and not features.has_select_for_no_key_update:
                        raise NotSupportedError("FOR NO KEY UPDATE is not supported on this database backend.")
                    for_update_part = self.connection.ops.for_update_sql(
                        nowait=nowait,
                        skip_locked=skip_locked,
                        of=self.get_select_for_update_of_arguments(),
                        no_key=no_key,
                    )

                if for_update_part and features.for_update_after_from:
                    result.append(for_update_part)

                if where:
                    result.append("WHERE %s" % where)
                    params.extend(w_params)

                grouping = []
                for g_sql, g_params in group_by:
                    grouping.append(g_sql)
                    params.extend(g_params)
                if grouping:
                    if distinct_fields:
                        raise NotImplementedError("annotate() + distinct(fields) is not implemented.")
                    order_by = order_by or self.connection.ops.force_no_ordering()
                    result.append("GROUP BY %s" % ", ".join(grouping))
                    if self._meta_ordering:
                        order_by = None
                if having:
                    if not grouping:
                        result.extend(self.connection.ops.force_group_by())
                    result.append("HAVING %s" % having)
                    params.extend(h_params)

            if self.query.explain_info:
                result.insert(
                    0,
                    self.connection.ops.explain_query_prefix(
                        self.query.explain_info.format,
                        **self.query.explain_info.options,
                    ),
                )

            if order_by:
                ordering = []
                for _, (o_sql, o_params, _) in order_by:
                    ordering.append(o_sql)
                    params.extend(o_params)
                order_by_sql = "ORDER BY %s" % ", ".join(ordering)
                if combinator and features.requires_compound_order_by_subquery:
                    result = ["SELECT * FROM (", *result, ")", order_by_sql]
                else:
                    result.append(order_by_sql)

            if with_limit_offset:
                result.append(self.connection.ops.limit_offset_sql(self.query.low_mark, self.query.high_mark))

            if for_update_part and not features.for_update_after_from:
                result.append(for_update_part)

            if self.query.subquery and extra_select:
                # If the query is used as a subquery, the extra selects would
                # result in more columns than the left-hand side expression is
                # expecting. This can happen when a subquery uses a combination
                # of order_by() and distinct(), forcing the ordering
                # expressions to be selected as well. Wrap the query in another
                # subquery to exclude extraneous selects.
                sub_selects = []
                sub_params = []
                for index, (select, _, alias) in enumerate(self.select, start=1):
                    if alias:
                        sub_selects.append(
                            "%s.%s"
                            % (
                                self.connection.ops.quote_name("subquery"),
                                self.connection.ops.quote_name(alias),
                            ),
                        )
                    else:
                        select_clone = select.relabeled_clone({select.alias: "subquery"})
                        subselect, subparams = select_clone.as_sql(self, self.connection)
                        sub_selects.append(subselect)
                        sub_params.extend(subparams)
                return "SELECT %s FROM (%s) subquery" % (
                    ", ".join(sub_selects),
                    " ".join(result),
                ), tuple(sub_params + params)

            return " ".join(result), tuple(params)
        finally:
            # Finally do cleanup - get rid of the joins we created above.
            self.query.reset_refcounts(refcounts_before)

    async def aresults_iter(
        self,
        results=None,
        tuple_expected=False,
        chunked_fetch=False,
        chunk_size=GET_ITERATOR_CHUNK_SIZE,
    ):
        """Return an iterator over the results from executing this query."""
        if results is None:
            results = await self.aexecute_sql(MULTI, chunked_fetch=chunked_fetch, chunk_size=chunk_size)
        fields = [s[0] for s in self.select[0 : self.col_count]]
        converters = self.get_converters(fields)
        rows = chain.from_iterable(results)
        if converters:
            rows = self.apply_converters(rows, converters)
        if self.has_composite_fields(fields):
            rows = self.composite_fields_to_tuples(rows, fields)
        if tuple_expected:
            rows = map(tuple, rows)
        return rows

    async def ahas_results(self):
        """
        Backends (e.g. NoSQL) can override this in order to use optimized
        versions of "query has any results."
        """
        return bool(await self.aexecute_sql(SINGLE))

    async def aexecute_sql(
        self,
        result_type=MULTI,
        chunked_fetch=False,
        chunk_size=GET_ITERATOR_CHUNK_SIZE,
    ):
        """
        Run the query against the database and return the result(s). The
        return value depends on the value of result_type.

        When result_type is:
        - MULTI: Retrieves all rows using fetchmany(). Wraps in an iterator for
           chunked reads when supported.
        - SINGLE: Retrieves a single row using fetchone().
        - ROW_COUNT: Retrieves the number of rows in the result.
        - CURSOR: Runs the query, and returns the cursor object. It is the
           caller's responsibility to close the cursor.
        """
        result_type = result_type or NO_RESULTS
        try:
            sql, params = self.as_sql()
            if not sql:
                raise EmptyResultSet
        except EmptyResultSet:
            if result_type == MULTI:
                return []
            return None
        if chunked_fetch:
            cursor = await self.connection.chunked_cursor()
        else:
            cursor = await self.connection.cursor()
        try:
            await cursor.execute(sql, params)
        except Exception:
            # Might fail for server-side cursors (e.g. connection closed)
            await cursor.close()
            raise

        if result_type == ROW_COUNT:
            try:
                return cursor.rowcount
            finally:
                await cursor.close()
        if result_type == CURSOR:
            # Give the caller the cursor to process and close.
            return cursor
        if result_type == SINGLE:
            try:
                val = await cursor.fetchone()
                if val:
                    return val[0 : self.col_count]
                return val
            finally:
                # done with the cursor
                await cursor.close()
        if result_type == NO_RESULTS:
            await cursor.close()
            return None

        result = cursor_iter(
            cursor,
            self.connection.features.empty_fetchmany_value,
            self.col_count if self.has_extra_select else None,
            chunk_size,
        )
        if not chunked_fetch or not self.connection.features.can_use_chunked_reads:
            # If we are using non-chunked reads, we return the same data
            # structure as normally, but ensure it is all read into memory
            # before going any further. Use chunked_fetch if requested,
            # unless the database doesn't support it.
            return [i async for i in result]
        return result

    async def aexplain_query(self):
        result = list(await self.aexecute_sql())
        # Some backends return 1 item tuples with strings, and others return
        # tuples with integers and strings. Flatten them out into strings.
        format_ = self.query.explain_info.format
        output_formatter = json.dumps if format_ and format_.lower() == "json" else str
        for row in result:
            for value in row:
                if not isinstance(value, str):
                    yield " ".join([output_formatter(c) for c in value])
                else:
                    yield value


class SQLInsertCompiler(DjangoSQLInsertCompiler, SQLCompiler):
    returning_fields = None
    returning_params = ()

    def as_sql(self):
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.get_meta()
        insert_statement = self.connection.ops.insert_statement(
            on_conflict=self.query.on_conflict,
        )
        result = ["%s %s" % (insert_statement, qn(opts.db_table))]

        if fields := list(self.query.fields):
            from django.db.models.expressions import DatabaseDefault

            supports_default_keyword_in_bulk_insert = self.connection.features.supports_default_keyword_in_bulk_insert
            value_cols = []
            for field in list(fields):
                field_prepare = partial(self.prepare_value, field)
                field_pre_save = partial(self.pre_save_val, field)
                field_values = [field_prepare(field_pre_save(obj)) for obj in self.query.objs]

                if not field.has_db_default():
                    value_cols.append(field_values)
                    continue

                # If all values are DEFAULT don't include the field and its
                # values in the query as they are redundant and could prevent
                # optimizations. This cannot be done if we're dealing with the
                # last field as INSERT statements require at least one.
                if len(fields) > 1 and all(isinstance(value, DatabaseDefault) for value in field_values):
                    fields.remove(field)
                    continue

                if supports_default_keyword_in_bulk_insert:
                    value_cols.append(field_values)
                    continue

                # If the field cannot be excluded from the INSERT for the
                # reasons listed above and the backend doesn't support the
                # DEFAULT keyword each values must be expanded into their
                # underlying expressions.
                prepared_db_default = field_prepare(field.db_default)
                field_values = [
                    (prepared_db_default if isinstance(value, DatabaseDefault) else value) for value in field_values
                ]
                value_cols.append(field_values)
            value_rows = list(zip(*value_cols))
            result.append("(%s)" % ", ".join(qn(f.column) for f in fields))
        else:
            # No fields were specified but an INSERT statement must include at
            # least one column. This can only happen when the model's primary
            # key is composed of a single auto-field so default to including it
            # as a placeholder to generate a valid INSERT statement.
            value_rows = [[self.connection.ops.pk_default_value()] for _ in self.query.objs]
            fields = [None]
            result.append("(%s)" % qn(opts.pk.column))

        # Currently the backends just accept values when generating bulk
        # queries and generate their own placeholders. Doing that isn't
        # necessary and it should be possible to use placeholders and
        # expressions in bulk inserts too.
        can_bulk = not self.returning_fields and self.connection.features.has_bulk_insert

        placeholder_rows, param_rows = self.assemble_as_sql(fields, value_rows)

        on_conflict_suffix_sql = self.connection.ops.on_conflict_suffix_sql(
            fields,
            self.query.on_conflict,
            (f.column for f in self.query.update_fields),
            (f.column for f in self.query.unique_fields),
        )
        if self.returning_fields and self.connection.features.can_return_columns_from_insert:
            if self.connection.features.can_return_rows_from_bulk_insert:
                result.append(self.connection.ops.bulk_insert_sql(fields, placeholder_rows))
                params = param_rows
            else:
                result.append("VALUES (%s)" % ", ".join(placeholder_rows[0]))
                params = [param_rows[0]]
            if on_conflict_suffix_sql:
                result.append(on_conflict_suffix_sql)
            # Skip empty r_sql to allow subclasses to customize behavior for
            # 3rd party backends. Refs #19096.
            r_sql, self.returning_params = self.connection.ops.returning_columns(self.returning_fields)
            if r_sql:
                result.append(r_sql)
                params += [self.returning_params]
            return [(" ".join(result), tuple(chain.from_iterable(params)))]

        if can_bulk:
            result.append(self.connection.ops.bulk_insert_sql(fields, placeholder_rows))
            if on_conflict_suffix_sql:
                result.append(on_conflict_suffix_sql)
            return [(" ".join(result), tuple(p for ps in param_rows for p in ps))]
        if on_conflict_suffix_sql:
            result.append(on_conflict_suffix_sql)
        return [
            (" ".join([*result, "VALUES (%s)" % ", ".join(p)]), vals) for p, vals in zip(placeholder_rows, param_rows)
        ]

    async def aexecute_sql(self, returning_fields=None):
        assert not (
            returning_fields
            and len(self.query.objs) != 1
            and not self.connection.features.can_return_rows_from_bulk_insert
        )
        opts = self.query.get_meta()
        self.returning_fields = returning_fields
        cols = []
        async with await self.connection.cursor() as cursor:
            for sql, params in self.as_sql():
                await cursor.execute(sql, params)
            if not self.returning_fields:
                return []
            obj_len = len(self.query.objs)
            if (self.connection.features.can_return_rows_from_bulk_insert and obj_len > 1) or (
                self.connection.features.can_return_columns_from_insert and obj_len == 1
            ):
                rows = await self.connection.ops.fetch_returned_rows(cursor, self.returning_params)
                cols = [field.get_col(opts.db_table) for field in self.returning_fields]
            elif returning_fields and isinstance(returning_field := returning_fields[0], AutoField):
                cols = [returning_field.get_col(opts.db_table)]
                rows = [
                    (
                        self.connection.ops.last_insert_id(
                            cursor,
                            opts.db_table,
                            returning_field.column,
                        ),
                    ),
                ]
            else:
                # Backend doesn't support returning fields and no auto-field
                # that can be retrieved from `last_insert_id` was specified.
                return []
        converters = self.get_converters(cols)
        if converters:
            rows = self.apply_converters(rows, converters)
        return list(rows)


class SQLDeleteCompiler(DjangoSQLDeleteCompiler, SQLCompiler):
    @cached_property
    def single_alias(self):
        # Ensure base table is in aliases.
        self.query.get_initial_alias()
        return sum(self.query.alias_refcount[t] > 0 for t in self.query.alias_map) == 1

    @classmethod
    def _expr_refs_base_model(cls, expr, base_model):
        if isinstance(expr, Query):
            return expr.model == base_model
        if not hasattr(expr, "get_source_expressions"):
            return False
        return any(cls._expr_refs_base_model(source_expr, base_model) for source_expr in expr.get_source_expressions())

    @cached_property
    def contains_self_reference_subquery(self):
        return any(
            self._expr_refs_base_model(expr, self.query.model)
            for expr in chain(self.query.annotations.values(), self.query.where.children)
        )


class SQLUpdateCompiler(DjangoSQLUpdateCompiler, SQLCompiler):
    returning_fields = None
    returning_params = ()

    async def aexecute_sql(self, result_type):
        """
        Execute the specified update. Return the number of rows affected by
        the primary update query. The "primary update query" is the first
        non-empty query that is executed. Row counts for any subsequent,
        related queries are not available.
        """
        row_count = await super().aexecute_sql(result_type)
        is_empty = row_count is None
        row_count = row_count or 0

        for query in self.query.get_related_updates():
            # If the result_type is NO_RESULTS then the aux_row_count is None.
            aux_row_count = await query.get_compiler(self.using).aexecute_sql(result_type)
            if is_empty and aux_row_count:
                # Returns the row count for any related updates as the number
                # of rows updated.
                row_count = aux_row_count
                is_empty = False
        return row_count

    async def aexecute_returning_sql(self, returning_fields):
        """
        Execute the specified update and return rows of the returned columns
        associated with the specified returning_field if the backend supports
        it.
        """
        if self.query.get_related_updates():
            raise NotImplementedError("Update returning is not implemented for queries with related updates.")

        if not returning_fields or not self.connection.features.can_return_rows_from_update:
            row_count = await self.aexecute_sql(ROW_COUNT)
            return [()] * row_count

        self.returning_fields = returning_fields
        async with await self.connection.cursor() as cursor:
            sql, params = self.as_sql()
            await cursor.execute(sql, params)
            rows = await self.connection.ops.fetch_returned_rows(cursor, self.returning_params)
        opts = self.query.get_meta()
        cols = [field.get_col(opts.db_table) for field in self.returning_fields]
        converters = self.get_converters(cols)
        if converters:
            rows = self.apply_converters(rows, converters)
        return list(rows)


class SQLAggregateCompiler(DjangoSQLAggregateCompiler, SQLCompiler):
    pass


async def cursor_iter(cursor, sentinel, col_count, itersize):
    """
    Yield blocks of rows from a cursor and ensure the cursor is closed when
    done.
    """

    async def fetchmany_iter():
        while True:
            rows = await cursor.fetchmany(itersize)
            if rows == sentinel:
                break
            yield rows

    try:
        async for rows in fetchmany_iter():
            yield rows if col_count is None else [r[:col_count] for r in rows]
    finally:
        await cursor.close()
