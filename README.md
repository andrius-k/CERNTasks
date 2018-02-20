# CERN Tasks

Various CMS data aggregation related tasks.

## How to run

after cloning please run `./init`. This will clone `CMSSpark` dependency which is required to run scripts in this repository. This will also clone wiki repository for submitting automatic reports.

Afterwards please go to file `CMSSpark/src/python/CMSSpark/phedex.py` and change `.option("header", "false")` to `.option("header", "true")` on line 113. This will make sure we get the data with headers of csv columns. This step in only necessary when running task1.

### How to setup hadoop directory

In `aggregate` file in each task please change hadoop paths to point to your user directory. Basically change *akirilov* to your CERN username.

## Running task 1

### Retrieving and aggregating data

In order to retrieve and aggregate PhEDEx data please run `./aggregate`

Data will be aggregated to file `df.csv`

### Analysing data

In order to analyze data please run `python analyse.py`. This will print the number of PhEDEx sites and sum of each of their sizes. `figure.pdf` with bar plot will be created to visualize the data.

## Running task 2

### Retrieving and aggregating data

In order to retrieve and aggregate PhEDEx and DBS data please run `./aggregate`

### Analysing data

In order to analyze data please run `python analyse.py`. This will generate all plots and push automatically generated report to wiki page: https://github.com/andrius-k/CERNTasks/wiki/Phedex_DBS_Reports

### How to automatically commit report

If `--commit` argument is passed to `analyse.py` script, automatically generated report will be commited to wiki of this repository. This requires authentication.

Automatically generated report will be placed here: `CMSTasks.wiki/CMS_Reports.md`

## Running task 3

### Retrieving and aggregating data

In order to retrieve and aggregate PhEDEx and DBS campaign data please run `./aggregate`

### Analysing data

In order to analyze data please run `python analyse.py`. This will generate all plots and push automatically generated report to wiki page: https://github.com/andrius-k/CERNTasks/wiki/CMS_Campaign_Reports

### How to automatically commit report

If `--commit` argument is passed to `analyse.py` script, automatically generated report will be commited to wiki of this repository. This requires authentication.

Automatically generated report will be placed here: `CMSTasks.wiki/CMS_Campaign_Reports.md`
