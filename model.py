import warnings

from dask.dataframe import read_csv
import pandas as pd
import numpy as np
from dask.dataframe.core import meta_warning

"""
Dataset info: 
Size: 558,837 x 16
Features: 'year', 'make', 'model', 'trim', 'body', 'transmission', 'vin', 'state', 'condition', 'odometer', 'color', 
'interior', 'seller', 'mmr', 'sellingprice', 'saledate'
"""

# CONSTANTS
FILEPATH = 'dataset/car_prices.csv'
desired_width = 320


def set_display():
    pd.set_option('display.width', desired_width)
    np.set_printoptions(linewidth=desired_width)
    pd.set_option('display.max_columns', 10)
    return


def import_data():
    df = read_csv(path=FILEPATH, dtype={'condition': 'float64',
                                        'mmr': 'float64',
                                        'odometer': 'float64',
                                        'sellingprice': 'float64'})
    return df


def explore_distinct_values(df, cols):
    print('**************************************************************')
    print("Get Distinct Values...\n")

    for col in cols:
        df[col] = df[col].apply(lambda x: str(x).capitalize(), meta=(col, 'string'))
        print(f'\nFor column {col}, ')
        unique_values = sorted(set(df[col].unique().compute()))
        print(f'{len(unique_values)} values encountered')
        print("------>", ", ".join(unique_values))
        print('----------------------------------------------------------')
    print('done')


def perform_eda(df):
    print(df.info())
    print(f'The columns are - ', ", ".join(df.columns))
    print("eda done")

    print(df.dtypes)

    print("eda done")

    # find distinct values
    cols = ['make', 'model', 'trim', 'body', 'transmission', 'state', 'color',
            'interior']
    explore_distinct_values(df, cols)


def main():
    set_display()
    df = import_data()
    perform_eda(df)
    ### Next step - remove irrelevant columns with garbage values, missing values, ---, etc

    print("test")
    print("Program Execution Complete..")


if __name__ == '__main__':
    main()
