from tabulate import tabulate


def print_dataframe(df):
    print(tabulate(df, headers='keys', tablefmt='psql'))
