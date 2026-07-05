from typing import Any, Callable
from urllib.parse import urlencode

from flask import request

DEFAULT_LIMIT = 20
MAX_LIMIT = 2000


def get_total_rows(conn, table_name: str) -> int:
    """Returns the total number of rows in a table."""
    result = conn.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()
    return result[0] if result else 0


def get_pagination():
    limit = request.args.get('limit', DEFAULT_LIMIT, type=int)
    limit = max(min(limit, MAX_LIMIT), 0)

    offset = request.args.get('offset', 0, type=int)
    offset = max(offset, 0)

    return limit, offset


def build_next_url(limit: int, offset: int, total: int) -> str | None:

    args = request.args.to_dict()

    next_url = None
    if offset + limit < total:
        args['limit'] = limit
        args['offset'] = offset + limit
        next_url = f'{request.path}?{urlencode(args)}'

    return next_url


def build_paginated_response(
    items: list, total: int, limit: int, offset: int
) -> dict[str, Any]:

    return {
        'data': items,
        'pagination': {
            'limit': limit,
            'offset': offset,
            'count': len(items),
            'total': total,
        },
    }


def paginate_query(
    *,
    conn,
    data_query: str,
    count_query: str,
    params: list[Any] | None = None,
    row_factory: Callable[[tuple], dict],
) -> tuple[dict[str, Any], str | None]:
    params = params or []

    limit, offset = get_pagination()

    total = conn.execute(
        count_query,
        params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""
        {data_query}
        LIMIT ?
        OFFSET ?
        """,
        params + [limit, offset],
    ).fetchall()

    data = [row_factory(row) for row in rows]

    response = build_paginated_response(
        items=data,
        total=total,
        limit=limit,
        offset=offset,
    )
    next_url = build_next_url(limit=limit, offset=offset, total=total)

    return response, next_url
