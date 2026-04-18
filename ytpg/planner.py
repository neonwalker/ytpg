from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable

import sqlglot
import sqlglot.expressions as exp

from ytpg.errors import PGError
from ytpg.manifest import resolve_table, resolve_table_segments

logger = logging.getLogger(__name__)


@dataclass
class JoinSpec:
    kind: str
    table_name: str
    alias: str
    video_id: str
    segments: list[str] = field(default_factory=list)
    on: Callable[[dict], bool] | None = None


@dataclass
class FetchAndMaterializeOp:
    table_name: str
    video_id: str
    where: Callable[[dict], bool] | None = None
    columns: list[str] | None = None
    limit: int | None = None
    orderings: list[tuple[str, bool, bool]] = field(default_factory=list)
    aggregate: str | None = None
    segments: list[str] = field(default_factory=list)
    alias: str = ""
    joins: list[JoinSpec] = field(default_factory=list)


@dataclass
class AppendCueOp:
    table_name: str
    video_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    segments: list[str] = field(default_factory=list)


@dataclass
class CreateTableOp:
    table_name: str
    columns: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DropTableOp:
    table_name: str
    video_id: str


@dataclass
class UpdateManifestOp:
    action: str
    table_name: str
    video_id: str | None = None


@dataclass
class ExplainOp:
    inner: list
    analyze: bool = False


def _eval_literal(node: exp.Expression) -> Any:
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        try:
            if "." in node.this:
                return float(node.this)
            return int(node.this)
        except ValueError:
            return node.this
    if isinstance(node, exp.Boolean):
        return node.this
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Neg):
        return -_eval_literal(node.this)
    return node.sql()


def _like_to_regex(pattern: str, case_insensitive: bool = False) -> re.Pattern:
    out = ["^"]
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    out.append("$")
    flags = re.DOTALL | (re.IGNORECASE if case_insensitive else 0)
    return re.compile("".join(out), flags)


def _coerce_literal(val: Any, type_str: str | None) -> Any:
    if type_str is None:
        return val
    from ytpg.schema import coerce_value
    return coerce_value(val, type_str)


Resolver = Callable[[exp.Expression], tuple[str, str | None]]


def _make_single_resolver(schema_types: dict[str, str] | None) -> Resolver:
    def resolve(node: exp.Expression) -> tuple[str, str | None]:
        if not isinstance(node, exp.Column):
            return (node.sql(), None)
        return (node.name, (schema_types or {}).get(node.name))
    return resolve


def _make_joined_resolver(
    base_alias: str,
    schemas_by_alias: dict[str, list[dict] | None],
) -> Resolver:
    types_by_alias: dict[tuple[str, str], str] = {}
    cols_to_aliases: dict[str, list[str]] = {}
    for alias, schema in schemas_by_alias.items():
        if not schema:
            continue
        for c in schema:
            name = c["name"]
            types_by_alias[(alias, name)] = c.get("type", "TEXT")
            cols_to_aliases.setdefault(name, []).append(alias)

    def resolve(node: exp.Expression) -> tuple[str, str | None]:
        if not isinstance(node, exp.Column):
            return (node.sql(), None)
        name = node.name
        table = node.table
        if table:
            if table not in schemas_by_alias:
                raise PGError(
                    code="42P01",
                    message=f'missing FROM-clause entry for table "{table}"',
                )
            return (f"{table}.{name}", types_by_alias.get((table, name)))
        matches = cols_to_aliases.get(name, [])
        if len(matches) == 1:
            alias = matches[0]
            return (f"{alias}.{name}", types_by_alias.get((alias, name)))
        if len(matches) > 1:
            raise PGError(
                code="42702",
                message=f'column reference "{name}" is ambiguous',
            )
        return (f"{base_alias}.{name}", None)
    return resolve


_BINOPS: dict[type, Callable[[Any, Any], bool]] = {
    exp.EQ:  lambda a, b: a == b,
    exp.NEQ: lambda a, b: a != b,
    exp.GT:  lambda a, b: a is not None and b is not None and a > b,
    exp.LT:  lambda a, b: a is not None and b is not None and a < b,
    exp.GTE: lambda a, b: a is not None and b is not None and a >= b,
    exp.LTE: lambda a, b: a is not None and b is not None and a <= b,
}


def _build_predicate(
    node: exp.Expression,
    resolve: Resolver,
) -> Callable[[dict], bool]:
    if isinstance(node, exp.Paren):
        return _build_predicate(node.this, resolve)

    if isinstance(node, exp.Not):
        inner = _build_predicate(node.this, resolve)
        return lambda row: not inner(row)

    if isinstance(node, exp.And):
        left = _build_predicate(node.left, resolve)
        right = _build_predicate(node.right, resolve)
        return lambda row: left(row) and right(row)

    if isinstance(node, exp.Or):
        left = _build_predicate(node.left, resolve)
        right = _build_predicate(node.right, resolve)
        return lambda row: left(row) or right(row)

    if isinstance(node, exp.Is):
        col_key, _ = resolve(node.left)
        rhs = node.right
        negate = False
        if isinstance(rhs, exp.Not):
            negate = True
            rhs = rhs.this
        if isinstance(rhs, exp.Null):
            if negate:
                return lambda row, c=col_key: row.get(c) is not None
            return lambda row, c=col_key: row.get(c) is None
        raise PGError(
            code="0A000",
            message=f"Unsupported IS target: {node.sql()}",
        )

    if isinstance(node, exp.In):
        col_key, col_type = resolve(node.this)
        items = node.expressions or []
        if not items and node.args.get("query"):
            raise PGError(code="0A000", message="IN (subquery) is not supported")
        raw_vals = [_eval_literal(e) for e in items]
        vals = tuple(_coerce_literal(v, col_type) for v in raw_vals)
        return lambda row, c=col_key, vs=vals: row.get(c) in vs

    if isinstance(node, exp.Like) or isinstance(node, exp.ILike):
        col_key, _ = resolve(node.this)
        pattern = _eval_literal(node.expression)
        if not isinstance(pattern, str):
            raise PGError(
                code="0A000",
                message=f"LIKE pattern must be a string literal: {node.sql()}",
            )
        regex = _like_to_regex(pattern, case_insensitive=isinstance(node, exp.ILike))
        def _like_pred(row, c=col_key, r=regex):
            v = row.get(c)
            return isinstance(v, str) and bool(r.match(v))
        return _like_pred

    for klass, op_fn in _BINOPS.items():
        if isinstance(node, klass):
            left_key, left_type = resolve(node.left)
            if isinstance(node.right, exp.Column):
                right_key, _ = resolve(node.right)
                return lambda row, lk=left_key, rk=right_key, f=op_fn: (
                    f(row.get(lk), row.get(rk))
                )
            raw_val = _eval_literal(node.right)
            val = _coerce_literal(raw_val, left_type)
            return lambda row, lk=left_key, v=val, f=op_fn: f(row.get(lk), v)

    raise PGError(
        code="0A000",
        message=f"Unsupported WHERE predicate: {node.sql()}",
    )


def _extract_where(
    where_node,
    resolve: Resolver,
) -> Callable[[dict], bool] | None:
    if where_node is None:
        return None
    cond = where_node.this if isinstance(where_node, exp.Where) else where_node
    return _build_predicate(cond, resolve)


class Planner:
    def __init__(self, youtube, channel_id: str, backend=None) -> None:
        self._yt = youtube
        self._channel_id = channel_id
        self._backend = backend

    def _resolve(self, table_name: str) -> str:
        return resolve_table(self._yt, self._channel_id, table_name)

    def _resolve_segments(self, table_name: str) -> list[str]:
        return resolve_table_segments(self._yt, self._channel_id, table_name)

    def _get_schema(self, table_name: str) -> list[dict[str, Any]] | None:
        if self._backend is None:
            return None
        from ytpg.schema import get_table_schema
        try:
            return get_table_schema(self._backend, self._channel_id, table_name)
        except PGError:
            return None

    def _schema_types(self, schema: list[dict[str, Any]] | None) -> dict[str, str] | None:
        if schema is None:
            return None
        return {c["name"]: c.get("type", "TEXT") for c in schema}

    def plan(self, sql: str) -> list:
        sql = sql.strip().rstrip(";")

        analyze = False
        upper = sql.upper()
        if upper.startswith("EXPLAIN"):
            rest = sql[7:].lstrip()
            if rest.upper().startswith("ANALYZE"):
                analyze = True
                rest = rest[7:].lstrip()
            inner_ops = self.plan(rest)
            return [ExplainOp(inner=inner_ops, analyze=analyze)]

        try:
            ast = sqlglot.parse_one(
                sql,
                dialect="postgres",
                error_level=sqlglot.ErrorLevel.RAISE,
            )
        except sqlglot.errors.ParseError as exc:
            raise PGError(code="42601", message=f"Syntax error: {exc}") from exc

        if ast.find(exp.Subquery):
            raise PGError(code="0A000", message="Subqueries are not supported by ytpg.")
        if isinstance(ast, (exp.Update, exp.Delete)) and ast.find(exp.Join):
            raise PGError(
                code="0A000",
                message="JOIN is only supported in SELECT, not UPDATE/DELETE.",
            )

        return self._dispatch(ast)

    def _dispatch(self, ast: exp.Expression) -> list:
        if isinstance(ast, exp.Select):
            return self._plan_select(ast)
        if isinstance(ast, exp.Insert):
            return self._plan_insert(ast)
        if isinstance(ast, exp.Update):
            return self._plan_update(ast)
        if isinstance(ast, exp.Delete):
            return self._plan_delete(ast)
        if isinstance(ast, exp.Create):
            return self._plan_create(ast)
        if isinstance(ast, exp.Drop):
            return self._plan_drop(ast)
        raise PGError(
            code="0A000",
            message=f"Unsupported statement type: {type(ast).__name__}",
        )

    def _plan_select(self, ast: exp.Select) -> list:
        from_node = ast.find(exp.From)
        if from_node is None:
            raise PGError(code="42601", message="SELECT without FROM is not supported")

        base_table_expr = from_node.this
        if not isinstance(base_table_expr, exp.Table):
            raise PGError(code="0A000", message="Unsupported FROM clause")
        base_table_name = base_table_expr.name
        base_alias = base_table_expr.alias_or_name
        base_segments = self._resolve_segments(base_table_name)
        base_video_id = base_segments[-1]

        join_nodes: list = ast.args.get("joins") or []
        joins: list[JoinSpec] = []
        schemas_by_alias: dict[str, list[dict] | None] = {}
        base_schema = self._get_schema(base_table_name)
        schemas_by_alias[base_alias] = base_schema

        for j in join_nodes:
            t_expr = j.this
            if not isinstance(t_expr, exp.Table):
                raise PGError(code="0A000", message="Unsupported JOIN target")
            j_table = t_expr.name
            j_alias = t_expr.alias_or_name
            if j_alias in schemas_by_alias:
                raise PGError(
                    code="42712",
                    message=f'table name "{j_alias}" specified more than once',
                )
            j_segments = self._resolve_segments(j_table)
            j_video = j_segments[-1]
            side = (j.args.get("side") or "").upper()
            kind_arg = (j.args.get("kind") or "").upper()
            if side == "LEFT":
                kind = "LEFT"
            elif side == "RIGHT":
                kind = "RIGHT"
            elif side == "FULL":
                kind = "FULL"
            elif kind_arg == "CROSS":
                kind = "CROSS"
            else:
                kind = "INNER"
            schemas_by_alias[j_alias] = self._get_schema(j_table)
            joins.append(JoinSpec(
                kind=kind,
                table_name=j_table,
                alias=j_alias,
                video_id=j_video,
                segments=j_segments,
                on=None,
            ))

        has_joins = bool(joins)
        if has_joins:
            resolve = _make_joined_resolver(base_alias, schemas_by_alias)
        else:
            resolve = _make_single_resolver(self._schema_types(base_schema))

        for j_node, jspec in zip(join_nodes, joins):
            on_expr = j_node.args.get("on")
            if on_expr is not None:
                jspec.on = _build_predicate(on_expr, resolve)
            elif jspec.kind != "CROSS":
                raise PGError(
                    code="0A000",
                    message=f"{jspec.kind} JOIN without ON clause is not supported",
                )

        columns: list[str] | None = None
        aggregate: str | None = None
        projections = ast.expressions
        if projections:
            if len(projections) == 1 and isinstance(projections[0], exp.Star):
                columns = None
            else:
                cols: list[str] = []
                for proj in projections:
                    inner = proj.this if isinstance(proj, exp.Alias) else proj
                    if isinstance(inner, exp.AggFunc) or (
                        hasattr(exp, "Count") and isinstance(inner, exp.Count)
                    ):
                        aggregate = proj.sql()
                        cols = None  # type: ignore[assignment]
                        break
                    if isinstance(inner, exp.Column):
                        if has_joins:
                            key, _ = resolve(inner)
                            cols.append(key)
                        else:
                            cols.append(inner.name)
                    elif isinstance(proj, exp.Alias):
                        cols.append(proj.alias or proj.alias_or_name)
                    else:
                        name = proj.alias_or_name
                        if name:
                            cols.append(name)
                if cols is not None:
                    columns = cols if cols else None

        where_node = ast.find(exp.Where)
        where = _extract_where(where_node, resolve)

        orderings: list[tuple[str, bool, bool]] = []
        order_node = ast.find(exp.Order)
        if order_node and order_node.expressions:
            for ord_expr in order_node.expressions:
                if not isinstance(ord_expr, exp.Ordered):
                    continue
                col_node = ord_expr.this
                if isinstance(col_node, exp.Column):
                    if has_joins:
                        col, _ = resolve(col_node)
                    else:
                        col = col_node.name
                else:
                    col = col_node.sql()
                desc = bool(ord_expr.args.get("desc"))
                nulls_first = bool(ord_expr.args.get("nulls_first"))
                orderings.append((col, desc, nulls_first))

        limit: int | None = None
        limit_node = ast.find(exp.Limit)
        if limit_node:
            limit_expr = limit_node.args.get("expression") or limit_node.this
            if limit_expr is not None:
                try:
                    limit = int(limit_expr.this)
                except (ValueError, TypeError, AttributeError):
                    try:
                        limit = int(limit_expr.sql())
                    except (ValueError, TypeError):
                        pass

        return [FetchAndMaterializeOp(
            table_name=base_table_name,
            video_id=base_video_id,
            where=where,
            columns=columns,
            limit=limit,
            orderings=orderings,
            aggregate=aggregate,
            segments=base_segments,
            alias=base_alias,
            joins=joins,
        )]

    def _plan_insert(self, ast: exp.Insert) -> list:
        if isinstance(ast.this, exp.Schema):
            table_name = ast.this.this.name
            col_names = [c.name for c in ast.this.expressions]
        else:
            table_name = ast.this.name
            col_names = [c.name for c in (ast.args.get("columns") or [])]

        video_id = self._resolve(table_name)

        values_clause = ast.args.get("expression")
        if not values_clause or not isinstance(values_clause, exp.Values):
            raise PGError(code="42601", message="INSERT requires a VALUES clause")

        schema = self._get_schema(table_name)
        if not col_names and schema is not None:
            col_names = [c["name"] for c in schema]

        ops = []
        for row_tuple in values_clause.expressions:
            if isinstance(row_tuple, exp.Tuple):
                vals = [_eval_literal(v) for v in row_tuple.expressions]
            else:
                vals = [_eval_literal(row_tuple)]

            payload: dict[str, Any] = {}
            if col_names:
                for name, val in zip(col_names, vals):
                    payload[name] = val
            else:
                for i, val in enumerate(vals):
                    payload[f"col{i}"] = val

            payload["_op"] = "INSERT"
            if schema is not None:
                from ytpg.schema import coerce_payload, enforce_not_null
                payload = coerce_payload(schema, payload)
                enforce_not_null(schema, payload, check_missing=True)
            ops.append(AppendCueOp(table_name=table_name, video_id=video_id, payload=payload))

        return ops

    def _plan_update(self, ast: exp.Update) -> list:
        table_name = ast.this.name
        segments = self._resolve_segments(table_name)
        video_id = segments[-1]

        schema = self._get_schema(table_name)
        schema_types = self._schema_types(schema)
        resolve = _make_single_resolver(schema_types)
        where = _extract_where(ast.args.get("where"), resolve)

        set_values: dict[str, Any] = {}
        for eq in ast.args.get("expressions", []):
            if isinstance(eq, exp.EQ):
                col = eq.left.name if isinstance(eq.left, exp.Column) else eq.left.sql()
                set_values[col] = _eval_literal(eq.right)

        if schema is not None:
            from ytpg.schema import coerce_payload, enforce_not_null
            set_values = coerce_payload(schema, set_values)
            enforce_not_null(schema, set_values, check_missing=False)

        payload: dict[str, Any] = {"_op": "UPDATE", "_where": where}
        payload.update(set_values)
        return [AppendCueOp(
            table_name=table_name,
            video_id=video_id,
            payload=payload,
            segments=segments,
        )]

    def _plan_delete(self, ast: exp.Delete) -> list:
        table_expr = ast.this
        table_name = table_expr.name if isinstance(table_expr, exp.Table) else table_expr.this.name
        segments = self._resolve_segments(table_name)
        video_id = segments[-1]
        schema_types = self._schema_types(self._get_schema(table_name))
        resolve = _make_single_resolver(schema_types)
        where = _extract_where(ast.args.get("where"), resolve)
        payload: dict[str, Any] = {"_op": "DELETE", "_where": where}
        return [AppendCueOp(
            table_name=table_name,
            video_id=video_id,
            payload=payload,
            segments=segments,
        )]

    def _plan_create(self, ast: exp.Create) -> list:
        if ast.args.get("kind", "").upper() != "TABLE":
            raise PGError(code="0A000", message="Only CREATE TABLE is supported")

        table_name = ast.this.this.name if isinstance(ast.this, exp.Schema) else ast.this.name

        columns: list[dict[str, Any]] = []
        schema = ast.this if isinstance(ast.this, exp.Schema) else None
        if schema:
            from ytpg.schema import parse_column_defs
            columns = parse_column_defs(schema.expressions)

        return [CreateTableOp(table_name=table_name, columns=columns)]

    def _plan_drop(self, ast: exp.Drop) -> list:
        kind = ast.args.get("kind")
        kind_str = kind.upper() if isinstance(kind, str) else (kind.name if kind else "")
        if kind_str != "TABLE":
            raise PGError(code="0A000", message="Only DROP TABLE is supported")
        table_node = ast.this if isinstance(ast.this, exp.Table) else ast.find(exp.Table)
        if table_node is None:
            raise PGError(code="42601", message="DROP TABLE: could not parse table name")
        table_name = table_node.name
        video_id = self._resolve(table_name)
        return [DropTableOp(table_name=table_name, video_id=video_id)]
