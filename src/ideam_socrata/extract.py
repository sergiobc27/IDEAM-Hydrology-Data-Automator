from .config import CLIENT, LIMIT


def iter_socrata_pages(dataset_id, retry_func, where_str=None, select=None, order=":id", limit=LIMIT):
    """Yield pages from Socrata without accumulating the full dataset in memory."""
    offset = 0
    while True:
        data = retry_func(
            lambda: CLIENT.get(
                dataset_id,
                where=where_str,
                select=select,
                limit=limit,
                offset=offset,
                order=order,
            ),
            f"{dataset_id} offset={offset}",
        )
        if data is None:
            raise RuntimeError(f"Fallo critico consultando {dataset_id} offset={offset}")
        if not data:
            break
        yield data
        if len(data) < limit:
            break
        offset += limit
