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
        df[col] = df[col].apply(lambda x: str(x).lower(), meta=(col, 'string'))
        print(f'\nFor column {col}, ')
        unique_values = sorted(set(df[col].unique().compute()))
        print(f'{len(unique_values)} values encountered')
        print("------>", ", ".join(unique_values))
        print('----------------------------------------------------------')
    print('done')


def replace_values(df):
    rep_dict = {
        'chev truck': 'chevrolet',
        'dodge tk': 'dodge',
        'ford tk': 'ford',
        'ford truck': 'ford',
        'gmc truck': 'gmc',
        'hyundai tk': 'hyundai',
        'landrover': 'land rover',
        'mazda tk': 'mazda',
        'mercedes-b': 'mercedes',
        'Mercedes-benz': 'mercedes',
        'vw': 'volkswagen'

    }

    for key, value in rep_dict.items():
        df = df.replace(key, value)
    print((list(df['make'].unique().compute())))
    return df


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
    df = replace_values(df)
    perform_eda(df)
    ### Next step - remove irrelevant columns with garbage values, missing values, ---, etc

    print("test")
    print("Program Execution Complete..")


if __name__ == '__main__':
    main()
