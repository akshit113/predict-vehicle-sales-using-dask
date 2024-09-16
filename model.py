from dask.dataframe import read_csv
import pandas as pd
import numpy as np

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
    print(len(df))
    print(list(df.columns))
    return df


def main():
    set_display()
    df = import_data()
    print("test")
    print("Program Execution Complete..")


if __name__ == '__main__':
    main()
