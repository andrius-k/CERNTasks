# Task 2

import pandas as pd

import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt
import os, shutil, subprocess
from subprocess import check_output

PHEDEX_PLOTS_PATH = 'phedex_plots/'
DBS_PLOTS_PATH = 'dbs_plots/'

report = ''

def append_report(lines):
    global report
    report = report + lines
    report = report + '\n'

def write_df_to_report(df):
    append_report('| Tier | Count | Size (TB) |')
    append_report('| ------- | ------ | ------ |')

    for index, row in df.iterrows():
        append_report(' | ' + index + ' | ' + str(int(row['tier_count'])) + ' | ' + str(round(row['sum_size'], 1)) + ' | ' )

def copy_directory(src, dest):
    # Delete destination first
    shutil.rmtree(dest)

    try:
        shutil.copytree(src, dest)
    # Directories are the same
    except shutil.Error as e:
        print('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print('Directory not copied. Error: %s' % e)

def create_plot_dirs():
    if not os.path.exists(PHEDEX_PLOTS_PATH):
        os.makedirs(PHEDEX_PLOTS_PATH)
    if not os.path.exists(DBS_PLOTS_PATH):
        os.makedirs(DBS_PLOTS_PATH)

def read_report_template():
    global report
    with open('../aggregation_template.md') as f: 
        report = f.read()

def write_report():
    global report
    with open('../CERNTasks.wiki/Phedex_DBS_Reports.md', 'w') as f: 
        f.write(report)

def commit_report():
    os.system('(cd ../CERNTasks.wiki/; git add -A; git commit -m "Auto-commiting report"; git push origin master)')

def make_plot(result, file_path):
    axes = result.plot(kind='bar', subplots=True, layout=(2,1), figsize=(8, 6), fontsize=6)

    axes[0][0].set_title('')
    axes[1][0].set_title('')
    axes[0][0].set_ylabel('Number')
    axes[1][0].set_ylabel('Terabytes')

    plt.xticks(rotation=45, horizontalalignment='right')
    plt.tight_layout()
    plot_filename = file_path + '_plot.jpg'
    plt.savefig(plot_filename, dpi=120)

    return plot_filename

def analyse_phedex_data():
    df = pd.read_csv('phedex_df.csv')
    # sites = df.groupby(df.site.str[:2])['site'].agg(lambda x: set(x)).index.tolist()
    sites = ['T1', 'T2', 'T3']

    append_report('## PhEDEx data')

    for site in sites:
        result = df[df.site.str[:2] == site] \
            .groupby(df.dataset.str.split('/').str[3]) \
            .agg({'size': 'sum', 'site': 'count'})

        result = result.rename(columns={'size': 'sum_size', 'site': 'tier_count'})

        # Bytes to terabytes
        result['sum_size'] = result['sum_size'] / 1000000000000

        append_report('### Site: ' + site)
        write_df_to_report(result)

        print 'Site: ' + site
        print(result)
        print ''

        plot_filename = make_plot(result, PHEDEX_PLOTS_PATH + site)

        append_report('### Plot')
        append_report('[[images/' + plot_filename + ']]')

    # Move plot files to wiki repo
    copy_directory(PHEDEX_PLOTS_PATH, '../CERNTasks.wiki/images/' + PHEDEX_PLOTS_PATH)

def analyse_dbs_data():
    df = pd.read_csv('dbs_df.csv')
    result = df.groupby(df.dataset.str.split('/').str[3]).agg({'size': 'sum', 'dataset': 'count'})

    result = result.rename(columns={'size': 'sum_size', 'dataset': 'tier_count'})

    # Bytes to terabytes
    result['sum_size'] = result['sum_size'] / 1000000000000

    append_report('## DBS data')
    write_df_to_report(result)

    print 'DBS data:'
    print(result)

    plot_filename = make_plot(result, DBS_PLOTS_PATH + 'dbs')

    append_report('### Plot')
    append_report('[[images/' + plot_filename + ']]')

    # Move plot file to wiki repo
    copy_directory(DBS_PLOTS_PATH, '../CERNTasks.wiki/images/' + DBS_PLOTS_PATH)

def aggregate_all_datastreams_info():
    append_report('## Sizes of all datastreams')
    append_report('| Stream | Size |')
    append_report('| ------- | ------ |')

    locations = { 
        'AAA (JSON) user logs accessing XrootD servers': 'hdfs:///project/monitoring/archive/xrootd/raw/gled', 
        'EOS (JSON) user logs accesses CERN EOS': 'hdfs:///project/monitoring/archive/eos/logs/reports/cms', 
        'HTCondor (JSON) CMS Jobs logs': 'hdfs:///project/awg/htcondor', # ???????
        # 'FTS (JSON) CMS FTS logs': 'hdfs://', 
        'CMSSW (Avro) CMSSW jobs': 'hdfs:///project/awg/cms/cmssw-popularity', 
        'JobMonitoring (Avro) CMS Dashboard DB snapshot': 'hdfs:///project/awg/cms/job-monitoring', 
        'WMArchive (Avro) CMS Workflows archive': 'hdfs:///cms/wmarchive/avro', 
        # 'ASO (CSV) CMS ASO accesses': 'hdfs://', 
        'DBS (CSV) CMS Data Bookkeeping snapshot': 'hdfs:///project/awg/cms/CMS_DBS3_PROD_GLOBAL/current', 
        'PhEDEx (CSV) CMS data location DB snapshot': 'hdfs:///project/awg/cms/phedex/block-replicas-snapshots'
    }

    print 'Size of all datastreams:'
    for name, location in locations.iteritems():
        out = check_output(['hadoop', 'fs', '-du', '-s', '-h', location])
        out = out.split(' ')
        size = out[0] + ' ' + out[1]
        append_report(' | ' + name + ' | ' + size + ' | ')
        print name + ': ' + size

def main():
    create_plot_dirs()
    read_report_template()
    
    analyse_phedex_data()
    analyse_dbs_data()
    aggregate_all_datastreams_info()

    write_report()
    commit_report()

if __name__ == '__main__':
    main()
    