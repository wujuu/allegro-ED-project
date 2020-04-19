import pandas as pd
import multiprocessing

from concurrent.futures import ThreadPoolExecutor


def _process_items(items):
    cpus = multiprocessing.cpu_count()
    chunks = _chunkify(items)

    with ThreadPoolExecutor(max_workers=cpus) as executor:
        futures = [executor.submit(_construct_items_df, chunk) for chunk in chunks]
        gathered_dfs = [future.result() for future in futures]

        return pd.concat(gathered_dfs, ignore_index=True)


def _filter_items_row(row):
    return pd.Series({
        'id': row['id'],
        'name': row['name'],
        'delivery_cost': float(row['delivery']['lowestPrice']['amount']),
        'cost': float(row['sellingMode']['price']['amount']),
        'stock': int(row['stock']['available']),
        'category_id': int(row['category']['id'])
    })


def _construct_items_df(items):
    return pd.DataFrame(items).apply(_filter_items_row, axis=1)


def _chunkify(items):
    cpus = multiprocessing.cpu_count()

    if len(items) < cpus:
        chunks = [[] for _ in range(cpus)]

        for i, item in enumerate(items):
            chunks[i].append(item)

        return chunks

    items_per_cpu = len(items) // cpus
    chunks = [items[x:x + items_per_cpu] for x in range(0, len(items), items_per_cpu)]

    if len(chunks) > cpus:
        i = 0
        for item in chunks[-1]:
            chunks[i].append(item)
            i = (i + 1) % cpus

    return chunks[:cpus]


def _construct_categories_df(categories):
    df = pd.DataFrame(categories)
    df = df.drop(columns=['options', 'parent', 'leaf'])
    df = df.set_index('name')
    return df


def _get_relevant_categories(root_items_df, threshold):
    unique_categories_counts = root_items_df.category_id.value_counts()
    condition = unique_categories_counts > threshold
    return unique_categories_counts[condition].index.values


def _glue_items(every_category_items):
    return pd.concat(every_category_items, ignore_index=True).drop_duplicates(subset=['id'], ignore_index=True)
