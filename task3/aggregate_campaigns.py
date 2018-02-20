#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
import os
import re
import sys
import time
import json
import argparse
from types import NoneType

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import elapsed_time

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process DBS+PhEDEx metadata"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'dbs_datasets.csv'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--tier", action="store",
            dest="tier", default="", help='Select datasets for given data-tier, use comma-separated list if you want to handle multiple data-tiers')
        self.parser.add_argument("--era", action="store",
            dest="era", default="", help='Select datasets for given acquisition era')
        self.parser.add_argument("--release", action="store",
            dest="release", default="", help='Select datasets for given CMSSW release')
        self.parser.add_argument("--cdate", action="store",
            dest="cdate", default="", help='Select datasets starting given creation date in YYYYMMDD format')
        self.parser.add_argument("--patterns", action="store",
            dest="patterns", default="", help='Select datasets patterns')
        self.parser.add_argument("--antipatterns", action="store",
            dest="antipatterns", default="", help='Select datasets antipatterns')
        msg = 'DBS instance on HDFS: global (default), phys01, phys02, phys03'
        self.parser.add_argument("--inst", action="store",
            dest="inst", default="global", help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select CMSSW data for specific date (YYYYMMDD)')

def extract_campaign(dataset):
    print dataset
    print type(dataset)
    return dataset.split('/')[2]

def run(fout, date, yarn=None, verbose=None, patterns=None, antipatterns=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)
    
    fromdate = '%s-%s-%s' % (date[:4], date[4:6], date[6:])
    todate = fromdate

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    tables.update(phedex_tables(sqlContext, verbose=verbose, fromdate=fromdate, todate=todate))
    phedex = tables['phedex_df']
    
    daf = tables['daf']
    ddf = tables['ddf']
    bdf = tables['bdf']
    fdf = tables['fdf']
    aef = tables['aef']
    pef = tables['pef']
    mcf = tables['mcf']
    ocf = tables['ocf']
    rvf = tables['rvf']

    print("### ddf from main", ddf)

    extract_campaign_udf = udf(lambda dataset: dataset.split('/')[2])

    dbs_fdf_cols = ['f_dataset_id', 'f_file_size']
    dbs_ddf_cols = ['d_dataset_id', 'd_dataset', ]

    fdf_df = fdf.select(dbs_fdf_cols)
    ddf_df = ddf.select(dbs_ddf_cols)

    # dataset, size
    dbs_df = fdf_df.join(ddf_df, fdf_df.f_dataset_id == ddf_df.d_dataset_id)\
                   .drop('f_dataset_id')\
                   .drop('d_dataset_id')\
                   .withColumnRenamed('d_dataset', 'dataset')\
                   .withColumnRenamed('f_file_size', 'size')

    # 1. campaign, dbs_size
    dbs_df = dbs_df.withColumn('campaign', extract_campaign_udf(dbs_df.dataset))\
                   .groupBy(['campaign'])\
                   .agg({'size':'sum'})\
                   .withColumnRenamed('sum(size)', 'dbs_size')\
                   .drop('dataset')

    # 2. campaign, phedex_size, site
    phedex_cols = ['dataset_name', 'block_bytes', 'node_name']
    phedex_df = phedex.select(phedex_cols)
    phedex_df = phedex_df.withColumn('campaign', extract_campaign_udf(phedex_df.dataset_name))\
                .groupBy(['campaign', 'dataset_name', 'node_name'])\
                .agg({'block_bytes':'sum'})\
                .withColumnRenamed('sum(block_bytes)', 'phedex_size')\
                .withColumnRenamed('node_name', 'site')\
                .drop('dataset_name')

    # 3. campaign, dbs_size, phedex_size, site
    dbs_phedex_df = dbs_df.join(phedex_df, dbs_df.campaign == phedex_df.campaign)\
                          .drop(dbs_df.campaign)

    # 4. campaign, total_dbs_size, total_phedex_size
    total_sizes_df = dbs_phedex_df.groupBy('campaign')\
                                  .agg({'dbs_size':'sum', 'phedex_size':'sum'})\
                                  .withColumnRenamed('sum(dbs_size)', 'total_dbs_size')\
                                  .withColumnRenamed('sum(phedex_size)', 'total_phedex_size')

    # 5. campaign, dbs_size, phedex_size, total_dbs_size, total_phedex_size, site
    result = dbs_phedex_df.join(total_sizes_df, dbs_phedex_df.campaign == total_sizes_df.campaign)\
                          .drop(total_sizes_df.campaign)\
                          .select('campaign', 'dbs_size', 'phedex_size', 'total_dbs_size', 'total_phedex_size', 'site')
    
    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        result.write.format("com.databricks.spark.csv")\
                    .option("header", "true").save(fout)

    ctx.stop()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    fout = opts.fout
    date = opts.date
    verbose = opts.verbose
    yarn = opts.yarn
    inst = opts.inst
    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    patterns = opts.patterns.split(',') if opts.patterns else []
    antipatterns = opts.antipatterns.split(',') if opts.antipatterns else []
    run(fout, date, yarn, verbose, patterns, antipatterns, inst)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()