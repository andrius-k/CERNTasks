# CERN Tasks

Various CMS data aggregation related tasks.

## How to run

after cloning please run `./init`. This will clone `CMSSpark` dependency which is required to run scripts in this repository. This will also clone wiki repository for submitting automatic reports.

Afterwards please go to file `CMSSpark/src/python/phedex.py` and change `.option("header", "false")` to `.option("header", "true")` on line 113. This will make sure we get the data with headers of csv columns.

## Running task 1

### Retrieving and aggregating data

In order to retrieve and aggregate PhEDEx data please run `./aggregate`

Data will be aggregated to file `df.csv`

### Analysing data

In order to analyze data please run `python analyse.py`. This will print the number of PhEDEx sites and sum of each of their sizes. `figure.pdf` with bar plot will be created to visualize the data.

## Running task 2

### Retrieving and aggregating data

In order to retrieve and aggregate PhEDEx data please run `./aggregate`

### Analysing data

In order to analyze data please run `python analyse.py`. This will generate all plots and push automatically generated report to wiki page: https://github.com/andrius-k/CERNTasks/wiki/Phedex_DBS_Reports
