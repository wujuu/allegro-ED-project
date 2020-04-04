from lib.querying import get_items
from lib.processing import filter_items


def main():
    items = get_items('zegarek')
    df = filter_items(items)

    print(df)


if __name__ == '__main__':
    main()
