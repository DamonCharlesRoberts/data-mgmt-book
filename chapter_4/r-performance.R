# Title: R Performance

# Notes:
  #* Description:
    #** R Performance Script
  #* Update
    #** 2023-11-17
    #** dcr

# Load libraries
box::use(
  tictoc[...]
  , polars[...]
  , duckdb[...]
  , DBI[...]
  , readr[read_delim]
  , arrow[read_parquet]
  , readxl[read_excel]
  , haven[read_dta, read_spss]
  , foreign[read.spss]
)
# Connect to database
con <- dbConnect(duckdb(), './data/chapter-4/performance-data.db')

# CSV
  #* Define file path
csv_path <- './data/2020-anes/anes.csv'

  #* Base R
    #** Clear tictoc log
tic.clearlog()
    #** Repeat extraction process 100 times
for (i in 1:100) {
  tic(i) # start timer
  anes_df <- read.csv(csv_path, sep=';') # load csv
  toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
base_r_csv_benchmark_log <- tic.log(format=FALSE) # grab the log
    #** Convert tictoc log to list
base_r_csv_benchmark <- lapply(
    base_r_csv_benchmark_log # by repeatedly taking the entries in the log
    , function (x) x$toc - x$tic # and calculating the difference between start and stop time
)

  #* Tidyverse
    #** Clear tictoc log
tic.clearlog()

    #** Repeat extraction process 100 times
for (i in 1:100) {
  tic(i) # start timer
  anes_df <- read_delim(csv_path, delim=';', show_col_types=FALSE) # load the file time
  toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
tidyverse_csv_benchmark_log <- tic.log(format=FALSE) # grab the log
    #** Convert tictoc log to list
tidyverse_csv_benchmark <- lapply(
    tidyverse_csv_benchmark_log # by repeatedly taking the entries in the log
    , function (x) x$toc - x$tic # and calculateing the difference between start and stop time
)

  #* Put profiling results into pl$DataFrame object
    #** Base R
base_csv_df <- pl$DataFrame(
    time = unlist(base_r_csv_benchmark)
    , library='Base'
    , language='R'
    , task='CSV'
)
    #** Tidyverse
tidyverse_csv_df <- pl$DataFrame(
    time = unlist(tidyverse_csv_benchmark)
    , library='Tidyverse'
    , language='R'
    , task='CSV'
)
    #** Concatenate
csv_df <- pl$concat(base_csv_df, tidyverse_csv_df)

# Parquet
  #* Define file path
parquet_path <- './data/2020-anes/anes.parquet'

  #* Base R
    #** Clear tictoc log
tic.clearlog()

    #** Repeat extraction process 100 times
for (i in 1:100) {
    tic(i) # start timer
    anes_df <- read_parquet(parquet_path) # load the file
    anes_df <- as.data.frame(anes_df) # convert it to a Base R data.frame
    toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
base_r_parquet_benchmark_log <- tic.log(format=FALSE) # log how long this loop took
    #** Convert tictoc log to list
base_r_parquet_benchmark <- lapply(
    base_r_parquet_benchmark_log # by repeatedly taking the entries in the log
    , function (x) x$toc - x$tic # and calculating the difference between the start and stop time
)

  #* Tidyverse
    #** Clear tictoc log
tic.clearlog()

    #** Repeat extraction process 100 times
for (i in 1:100) {
    tic(i) # start timer
    anes_df <- read_parquet(parquet_path) # load the file
    toc(i, quiet=TRUE) # stop the timer
}
    #** Format the tictoc log
tidyverse_parquet_benchmark_log <- tic.log(format=FALSE) # log how long this took
    #** Convert tictoc log to list
tidyverse_parquet_benchmark <- lapply(
    tidyverse_parquet_benchmark_log # by repeatedly taking the entries in the log
    , function (x) x$toc - x$tic
)

  #* Put profiling results into pl$DataFrame object
    #** Base R
base_parquet_df <- pl$DataFrame(
    time = unlist(base_r_parquet_benchmark)
    , library='Base'
    , language='R'
    , task='Parquet'
)
    #** Tidyverse
tidyverse_parquet_df <- pl$DataFrame(
    time = unlist(tidyverse_csv_benchmark)
    , library='Tidyverse'
    , language='R'
    , task='Parquet'
)
    #** Concatenate
parquet_df <- pl$concat(base_parquet_df, tidyverse_parquet_df)

# XLSX
  #* Define file path
xlsx_path <- './data/2020-anes/anes.xlsx'
  #* Base R
    #** Clear tictoc log
tic.clearlog()

    #** Repeat extraction process 100 times
for (i in 1:100) {
    tic(i) # start timer
    anes_df <- read_excel(xlsx_path) # load the file
    anes_df <- as.data.frame(anes_df) # convert it to a Base R data.frame
    toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
base_r_xlsx_benchmark_log <- tic.log(format=FALSE) # log how long this took
    #** Convert tictoc log to list
base_r_xlsx_benchmark <- lapply(
    base_r_xlsx_benchmark_log # by repeatedly taking the entries from the log
    , function (x) x$toc - x$tic # and calculating the difference between the start and stop time
)

  #* Tidyverse
    #** Clear tictoc log
tic.clearlog()

    #** Repeat extraction process 100 times
for (i in 1:100) {
    tic(i) # start timer
    anes_df <- read_excel(xlsx_path) # load the file
    toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
tidyverse_xlsx_benchmark_log <- tic.log(format=FALSE) # log how long this took
    #** Convert tictoc log to list
tidyverse_xlsx_benchmark <- lapply(
    tidyverse_xlsx_benchmark_log # by repeatedly taking the entrries from the log
    , function (x) x$toc - x$tic # and calculate the difference between the start and stop time
)
  #* Put profiling results into data.frame object
    #** Base R
base_xlsx_df <- pl$DataFrame(
    time = unlist(base_r_xlsx_benchmark)
    , library='Base'
    , language='R'
    , task='XLSX'
)
    #** Tidyverse
tidyverse_xlsx_df <- pl$DataFrame(
    time = unlist(tidyverse_xlsx_benchmark)
    , library='Tidyverse'
    , language='R'
    , task='XLSX'
)
    #** Concatenate
xlsx_df <- pl$concat(base_xlsx_df, tidyverse_xlsx_df)

# DTA
  #* Define file path
dta_path <- './data/2020-anes/anes.dta
'
  #* Base R
    #** Clear tictoc log
tic.clearlog()

    #** Repeat extraction process 100 times
for (i in 1:100) {
    tic(i) # start timer
    anes_df <- read_dta(dta_path) # extract dta file and place in tibble
    anes_df <- as.data.frame(anes_df) # convert to Base R data.frame
    toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
base_r_dta_benchmark_log <- toc.log(format=FALSE) # log how long this took
    #** Convert tictoc log to list
base_r_dta_benchmark <- lapply(
    base_r_dta_benchmark_log # by taking the entries from the log
    , function (x) x$toc - x$tic # and calculating the difference between the start and stop time
)

  #* Tidyverse
    #** Clear tictoc log
tic.clearlog()

    #** Repeat extraction process 100 times
for (i in 1:100) {
    tic(i) # start the timer
    anes_df <- read_dta(dta_path) # extract dta file and place in tibble
    toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
tidyverse_dta_benchmark_log <- toc.lgo(format=FALSE) # log how long this took
    #** Convert tictoc log to list
tidyverse_dta_benchmark <- lapply(
    tidyverse_dta_benchmark # by taking the entries from the log
    , function (x) x$toc - x$tic # and calculating the difference between the start and stop tim
)
  #* Put profiling results into data.frame object
    #** Base R
    #** Base R
base_dta_df <- pl$DataFrame(
    time = unlist(base_r_dta_benchmark)
    , library='Base'
    , language='R'
    , task='DTA'
)
    #** Tidyverse
tidyverse_dta_df <- pl$DataFrame(
    time = unlist(tidyverse_dta_benchmark)
    , library='Tidyverse'
    , language='R'
    , task='DTA'
)
    #** Concatenate
dta_df <- pl$concat(base_dta_df, tidyverse_dta_df)

# SAV
  #* Define file path
sav_path <- './data/2020-anes/anes.sav'
  #* Base R
    #** Clear tictoc log
tic.clearlog()
    #** Repeat extraction process 100 times
for (i in 1:100) {
  tic(i) # start timer
  anes_df <- read.spss(sav_path, to.data.frame=TRUE) # extract the data and place it in a Base R data.frame
  toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
base_r_sav_benchmark_log <- toc.log(format=FALSE) # log how long this took
    #** Convert tictoc log to list
base_r_sav_benchmark <- lapply(
    base_r_sav_benchmark_log # by repeatedly taking the entries in the log
    , function (x) x$toc - x$tic # and calculating the difference between the start and stop time
)

  #* Tidyverse
    #** Clear tictoc log
tic.clearlog()
    #** Repeat extraction process 100 times
for (i in 1:100) {
  tic(i) # start timer
  anes_df <- read_spss(sav_path) # extract the data and place it into a tibble
  toc(i, quiet=TRUE) # stop timer
}
    #** Format the tictoc log
tidyverse_sav_benchmark_log <- toc.log(format=FALSE) # log how long this took
    #** Convert tictoc log to list
tidyverse_sav_benchmark <- lapply(
    tidyverse_sav_benchmark # by repeatedly taking the entries in the log
    , function (x) x$toc - x$tic
)

  #* Put profiling results into data.frame object
    #** Base R
base_sav_df <- pl$DataFrame(
    time = unlist(base_r_sav_benchmark)
    , library='Base'
    , language='R'
    , task='SAV'
)
    #** Tidyverse
tidyverse_sav_df <- pl$DataFrame(
    time = unlist(tidyverse_sav_benchmark)
    , library='Tidyverse'
    , language='R'
    , task='SAV'
)
    #** Concatenate
sav_df <- pl$concat(base_sav_df, tidyverse_sav_df)

# Concatenate the dataframes together
performance_df <- pl$concat(csv_df, parquet_df, xlsx_df, dta_df, sav_df)

# Store as table
dbExecute(
  con
  , '
    INSERT INTO -- insert the following into the following table
      main
    (
        SELECT -- the following columns
            * -- all columns
        FROM
            performance_df -- from the performance_df pl.DataFrame
    )
    '

)

# Close connection
dbDisconnect(con)