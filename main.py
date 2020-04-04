from lib.querying import get_items
from lib.processing import process_items


def main():
    example_phrase = 'mydlo'

    items = get_items(example_phrase)
    df = process_items(items)

    print(df)


if __name__ == '__main__':
    main()
