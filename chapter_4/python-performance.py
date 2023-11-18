# Title: Python performance

# Notes:
    #* Description
        #** Performance of Libraries in Python
    #* Updated
        #** 2023-11-17
        #** dcr

# Load libraries
import timeit
import pandas as pd
import polars as pl
import duckdb as db
from statistics import mean, stdev
from random import seed

# Set seed
seed(121022)

# Connect to databases
    #* For ANES examples
anes_con = db.connect('./data/2020-anes/anes.db')
    #* For performance table
per_con = db.connect('./data/chapter-4/performance-data.db')

# Load spatial extension for duckdb
anes_con.execute(
    '''
        INSTALL spatial; -- only need to run this once per connection
        LOAD spatial; -- only need to run this once per connection
    '''
)

# CSV
    #* Define path for CSV file
csv_path = './data/2020-anes/anes.csv'
    #* Pandas
        #** Define function to be ran through
def pandas_extract_csv(path=csv_path):
    anes_df = pd.read_csv(path, delimiter=';')

        #* Repeat through the defined function 100 times and make list of time taken
pandas_extract_csv_time = timeit.timeit(
    'pandas_extract_csv()', # execute the function
    'from __main__ import pandas_extract_csv', # after importing it
    number=100 # run through it 100 times
)
    #* Polars
        #** Define function to be ran through
def polars_extract_csv(path=csv_path):
    anes_df = pl.read_csv(path, separator=';')

        #** Repeat through the defined function 100 times and make list of time taken
polars_extract_csv_time = timeit.timeit(
    'polars_extract_csv()', # execute the function
    'from __main__ import polars_extract_csv', # after importing it
    number=100 # run through it 100 times
)
    #* Duckdb
        #** Define function to be ran through
def duckdb_extract_csv():
    anes_con.execute(
        '''
            CREATE OR REPLACE TABLE -- create or replace a table
                main -- called main
            AS -- with the following data
                (
                    SELECT -- select the following columns
                        * -- all of the columns
                    FROM
                        read_csv_auto('./data/2020-anes/anes.csv') -- this specific file
                    /* 
                        Note:
                        - I am using a function by DuckDB that extracts the data from the csv for me
                        - I specify the file path inside the read_csv_auto function using single quotation marks.
                    */
                )
        '''
    )

        #** Repeat through the defined function 100 times and make a lits of time taken
duckdb_extract_csv_time = timeit.timeit(
    'duckdb_extract_csv()', # execute the function
    'from __main__ import duckdb_extract_csv', # after importing it
    number=100 # run through it 100 times
)

    #* Put profiling results in a dataframe
        #** Pandas
pandas_extract_csv_df = pl.DataFrame({
    "time":pandas_extract_csv_time,
    "library":"Pandas",
    "language":"Python",
    "task": "CSV"
})
        #** Polars
polars_extract_csv_df = pl.DataFrame({
    "time":polars_extract_csv_time,
    "library":"Polars",
    "language":"Python",
    "task": "CSV"
})
        #** Duckdb
duckdb_extract_csv_df = pl.DataFrame({
    "time":duckdb_extract_csv_time,
    "library":"DuckDB",
    "language":"SQL",
    "task": "CSV"
})
        #** Concatinate
csv_df = pl.concat([pandas_extract_csv_df, polars_extract_csv_df, duckdb_extract_csv_df])

    #* Put profiling results in a table
per_con.execute(
    '''
        CREATE OR REPLACE TABLE -- create or replace a table
            main -- called main
        AS -- using the following data
            (
                SELECT -- with the following columns
                    * -- all of the columns
                FROM
                    csv_df
            )
    '''
)
    #* Delete objects
del csv_path
del pandas_extract_csv, pandas_extract_csv_time, pandas_extract_csv_df
del polars_extract_csv, polars_extract_csv_time, polars_extract_csv_df
del duckdb_extract_csv, duckdb_extract_csv_time, duckdb_extract_csv_df
del csv_df

# Parquet
    #* Define path for parquet file
parquet_path = './data/2020-anes/anes.parquet'
    #* Pandas
        #** Define function to be ran through
def pandas_extract_parquet(path=parquet_path):
    anes_df = pd.read_parquet(path)
    pass
        #** Repeat through defined function 100 times to make list of time taken
pandas_extract_parquet_time = timeit.timeit(
    'pandas_extract_parquet()', # run through this function
    'from __main__ import pandas_extract_parquet', # after loading it
    number=100 # run through the function 100 times
)
    #* Polars
        #** Define function to be ran through
def polars_extract_parquet(path=parquet_path):
    anes_df = pd.read_parquet(path)
    pass
        #** Repeat through defined function 100 times to make list of time taken
polars_extract_parquet_time = timeit.timeit(
    'polars_extract_parquet()', # run through this funciton
    'from __main__ import polars_extract_parquet', # after loading it
    number=100 # run through the function 100 times
)
    #* Duckdb
        #** Define function to be ran through
def duckdb_extract_parquet():
    anes_con.execute(
        '''
            CREATE OR REPLACE TABLE -- create or replace a table
                main -- called main
            AS -- with the following data
                (
                    SELECT -- select the following columns
                        * -- all of them
                    FROM
                        read_parquet('./data/2020-anes/anes.parquet') -- the parquet file
                )
            /*
                NOTE:
                - I am using the read_parquet function provided by duckdb
                - I am specifying the file inside single quotations
            */
        '''
    )

        #** Repeat through defined function 100 times to make list of time taken
duckdb_extract_parquet_time = timeit.timeit(
    'duckdb_extract_parquet()', # run through the function
    'from __main__ import duckdb_extract_parquet', # after loading it
    number=100 # run through the function 100 times
)

    #* Put profiling results into pl.DataFrame object
        #** Pandas
pandas_extract_parquet_df = pl.DataFrame({
    "time":pandas_extract_parquet_time,
    "library":"Pandas",
    "language":"Python",
    "task": "CSV"
})
        #** Polars
polars_extract_parquet_df = pl.DataFrame({
    "time":polars_extract_parquet_time,
    "library":"Polars",
    "language":"Python",
    "task": "CSV"
})
        #** Duckdb
duckdb_extract_parquet_df = pl.DataFrame({
    "time":duckdb_extract_parquet_time,
    "library":"DuckDB",
    "language":"SQL",
    "task": "CSV"
})
        #** Concatenate
parquet_df = pl.concat([pandas_extract_parquet_df, polars_extract_parquet_df, duckdb_extract_parquet_df])
    
    #* Put profiling results in a table
per_con.execute(
    '''
        INSERT INTO -- insert the following into the following table
            main
        (
            SELECT -- the following columns
                * -- all columns
            FROM 
                parquet_df -- from the parquet_df pl.DataFrame
        )
    '''
)
    #* Delete objects
del parquet_path
del pandas_extract_parquet, pandas_extract_parquet_time, pandas_extract_parquet_df
del polars_extract_parquet, polars_extract_parquet_time, polars_extract_parquet_df
del duckdb_extract_parquet, duckdb_extract_parquet_time, duckdb_extract_parquet_df
del parquet_df

# XLSX
    #* Define path for XLSX file
xlsx_path = './data/2020-anes/anes.xlsx'

    #* Pandas
        #** Define function to be ran through
def pandas_extract_xlsx(path=xlsx_path):
    anes_df = pd.read_excel(path)
    pass

        #** Repeat through defined function 100 times to make list of time taken
pandas_extract_xlsx_time = timeit.timeit(
    'pandas_extract_xlsx()', # execute the function
    'from __main__ import pandas_extract_xlsx', # after importing it
    number=100 # run through it 100 times
)

    #* Polars
        #** Define function to be ran through
def polars_extract_xlsx(path=xlsx_path):
    anes_df = pl.read_excel(path)
    pass

        #** Repeat through defined function 100 times to make list of time taken
polars_extract_xlsx_time = timeit.timeit(
    'polars_extract_xlsx()', # execute the function
    'from __main__ import polars_extract_xlsx', # after importing it
    number=100 # run through it 100 times
)

    #* Duckdb
        #** Define function to be ran through
def duckdb_extract_xlsx():
    anes_con.execute(
        '''
            CREATE OR REPLACE TABLE -- create or replace a table
                main -- called main
            AS -- with the following data
                (
                    SELECT -- select the following columns
                        * -- all of the columns
                    FROM
                        st_read('./data/2020-anes/anes.xlsx', layer='Sheet1') -- the first sheet in anes.xlsx
                )
                /*
                    NOTE
                        - I am using the st_read function provided by the spatial extension
                        - Specify the file inside single quotation marks
                */
        '''
    )
    pass

        #** Repeat through defined function 100 times to make list of time taken
duckdb_extract_xlsx_time = timeit.timeit(
    'duckdb_extract_xlsx()', # execute the function
    'from __main__ import duckdb_extract_xlsx', # after importing it
    number=100 # run through it 100 times
)

    #* Put profiling results into pl.DataFrame object
        #** Pandas
pandas_extract_xlsx_df = pl.DataFrame({
    'time':pandas_extract_xlsx_time,
    'library':'Pandas',
    'language':'Python',
    'task':'XLSX'
})
        #** Polars
polars_extract_xlsx_df = pl.DataFrame({
    'time':polars_extract_xlsx_time,
    'library':'Polars',
    'language':'Python',
    'task':'XLSX'
})
        #** Duckdb
duckdb_extract_xlsx_df = pl.DataFrame({
    'time':duckdb_extract_xlsx_time,
    'library':'DuckDB',
    'language':'SQL',
    'task':'XLSX'
})
        #** Concatenate
xlsx_df = pl.concat([pandas_extract_xlsx_df, polars_extract_xlsx_df, duckdb_extract_xlsx_df])

    #* Put profiling results in a table
per_con.execute(
    '''
        INSERT INTO -- insert the following into the following table
            main
        (
            SELECT -- the following columns
                * -- all columns
            FROM
                xlsx_df -- from the xlsx_df pl.DataFrame
        )
    '''
)
    #* Delete objects
del xlsx_path
del pandas_extract_xlsx, pandas_extract_xlsx_time, pandas_extract_xlsx_df
del polars_extract_xlsx, polars_extract_xlsx_time, polars_extract_xlsx_df
del xlsx_df

# DTA
    #* Define path for DTA file
dta_path = './data/2020-anes/anes.dta'

    #* Pandas
        #** Define function to be ran through
def pandas_extract_dta(path=dta_path):
    anes_df = pd.read_stata(path, convert_categoricals=False)
    pass

        #** Repeat through defined function 100 times to make list of time taken
pandas_extract_dta_time = timeit.timeit(
    'pandas_extract_dta()', # run through this function
    'from __main__ import pandas_extract_dta', # after loading it
    number=100 # run through the function 100 times
)
    
    #* Polars
        #** Define function to be ran through
def polars_extract_dta(path=dta_path):
    anes_df = pd.read_stata(path, convert_categoricals=False)
    anes_df = pl.from_pandas(anes_df)
    pass

        #** Repeat through defined function 100 times to make list of time taken
polars_extract_dta_time = timeit.timeit(
    'polars_extract_dta()', # run through this function
    'from __main__ import polars_extract_dta', # after loading it
    number=100
)

    #* Duckdb
        #** Define function to be ran through
def duckdb_extract_dta(path=dta_path):
    anes_df = pd.read_stata(path, convert_categoricals=False)
    anes_con.execute(
        '''
            CREATE OR REPLACE TABLE -- create or replace a table
                main -- called main
            AS -- with the following data
                (
                    SELECT -- select the following columns
                        * -- all of them
                    FROM 
                        anes_df -- from the anes_df pandas.DataFrame object
                )
        '''
    )
    pass

        #** Repeat through defined function 100 times to make list of time taken
duckdb_extract_dta_time = timeit.timeit(
    'duckdb_extract_dta()', # run through this function
    'from __main__ import duckdb_extract_dta', # after loading it
    number=100
)

    #* Put profiling results into pl.DataFrame object
        #** Pandas
pandas_extract_dta_df = pl.DataFrame({
    'time':pandas_extract_dta_time,
    'library':'Pandas',
    'language':'Python',
    'task':'DTA'
})
        #** Polars
polars_extract_dta_df = pl.DataFrame({
    'time':polars_extract_dta_time,
    'library':'Polars',
    'language':'Python',
    'task':'DTA'
})
        #** Duckdb
duckdb_extract_dta_df = pl.DataFrame({
    'time':duckdb_extract_dta_time,
    'library':'DuckDB',
    'language':'SQL',
    'task':'DTA'
})
        #** Concatenate
dta_df = pl.concat([pandas_extract_dta_df, polars_extract_dta_df, duckdb_extract_dta_df])

    #* Put profiling results in a table
per_con.execute(
    '''
        INSERT INTO -- insert the following into the following table
            main
        (
            SELECT -- the following columns
                * -- all columns
            FROM
                dta_df -- from the dta_df pl.DataFrame
        )
    '''
)
    #* Delete objects
del dta_path
del pandas_extract_dta, pandas_extract_dta_time, pandas_extract_dta_df
del polars_extract_dta, polars_extract_dta_time, polars_extract_dta_df
del duckdb_extract_dta, duckdb_extract_dta_time, duckdb_extract_dta_df
del dta_df

# SAV
    #* Define path for sav file
sav_path = './data/2020-anes/anes.sav'

    #* Pandas
        #** Define function to be ran through
def pandas_extract_sav(path=sav_path):
    anes_df = pd.read_spss(sav_path, convert_categoricals=False)
    pass

        #** Repeat through defined function 100 times to make list of time taken
pandas_extract_sav_time = timeit.timeit(
    'pandas_extract_sav()', # run through the function
    'from __main__ import pandas_extract_sav', # after loading it
    number=100 # run through the function 100 times
)

    #* Polars
        #** Define function to be ran through
def polars_extract_sav(path=sav_path):
    anes_df = pd.read_spss(sav_path, convert_categoricals=False)
    anes_df = pl.from_pandas(anes_df)
    pass

        #** Repeat through defined function 100 times to make list of time taken
polars_extract_sav_time = timeit.timeit(
    'polars_extract_sav()', # run through the function
    'from __main__ import polars_extract_sav', # after loading it
    number=100 # run through the function 100 times
)

    #* Duckdb
        #** Define function to be ran through
def duckdb_extract_sav(path=sav_path):
    anes_df = pd.read_spss(sav_path, convert_categoricals=False)
    anes_con.execute(
        '''
            CREATE OR REPLACE TABLE -- create or replace a table
                main -- called main
            AS -- with the following data
            (
                SELECT -- select the following columns
                    * -- all of them
                FROM
                    anes_df -- from the anes_df pandas.DataFrame object
            )
        '''
    )
    pass
        #** Repeat through defined function 100 times to make list of time taken
duckdb_extract_sav_time = timeit.timeit(
    'duckdb_extract_sav()', # run through this function
    'from __main__ import duckdb_extract_sav', # after loading it
    number=100
)

    #* Put profiling results into pl.DataFrame object
        #** Pandas
pandas_extract_sav_df = pl.DataFrame({
    'time':pandas_extract_sav_time,
    'library':'Pandas',
    'language':'Python',
    'task':'SAV'
})
        #** Polars
polars_extract_sav_df = pl.DataFrame({
    'time':polars_extract_sav_time,
    'library':'Polars',
    'language':'Python',
    'task':'SAV'
})
        #** Duckdb
duckdb_extract_sav_df = pl.DataFrame({
    'time':duckdb_extract_sav_time,
    'library':'DuckDB',
    'language':'SQL',
    'task':'SAV'
})
        #** Concatenate
sav_df = pl.concat([pandas_extract_sav_df, polars_extract_sav_df, duckdb_extract_sav_df])

    #* Put profiling results in a table
per_con.execute(
    '''
        INSERT INTO -- insert the following into the following table
            main
        (
            SELECT -- the following columns
                * -- all columns
            FROM 
                sav_df -- from the sav_df pl.DataFrame
        )
    '''
)
    #* Delete objects
del sav_path
del pandas_extract_sav, pandas_extract_sav_time, pandas_extract_sav_df
del polars_extract_sav, polars_extract_sav_time, polars_extract_sav_df
del duckdb_extract_sav, duckdb_extract_sav_time, duckdb_extract_sav_df

# Disconnect from databases
anes_con.close()
per_con.close()