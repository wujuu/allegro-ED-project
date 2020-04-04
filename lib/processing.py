import pandas as pd
import multiprocessing

from concurrent.futures import ThreadPoolExecutor


def filter_items(items):
    """
    :param items:
        Raw url item data from query
    :return:
        Parsed DataFrame with relevant information only.
    """
    cpus = multiprocessing.cpu_count()
    chunks = _chunkify(items)

    with ThreadPoolExecutor(max_workers=cpus) as executor:
        futures = [executor.submit(_construct_df, chunk) for chunk in chunks]
        gathered_dfs = [future.result() for future in futures]

        return pd.concat(gathered_dfs, ignore_index=True)


def _filter_row(row):
    return pd.Series({
        'id': row['id'],
        'name': row['name'],
        'delivery_cost': float(row['delivery']['lowestPrice']['amount']),
        'cost': float(row['sellingMode']['price']['amount']),
        'stock': int(row['stock']['available']),
        'category_id': int(row['category']['id'])
    })


def _construct_df(items):
    return pd.DataFrame(items).apply(_filter_row, axis=1)


def _chunkify(items):
    cpus = multiprocessing.cpu_count()
    items_per_CPU = len(items) // cpus
    chunks = [items[x:x + items_per_CPU] for x in range(0, len(items), items_per_CPU)]

    if len(chunks) > cpus:
        i = 0
        for item in chunks[-1]:
            chunks[i].append(item)
            i = (i + 1) % cpus

    return chunks[:cpus]



