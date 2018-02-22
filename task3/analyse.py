import pandas as pd

import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt
from subprocess import check_output
from pandas import pivot_table
import os
import shutil
import operator
import argparse

PLOTS_PATH = 'campaign_plots/'

report = ''

def append_report(lines):
    global report
    report = report + lines
    report = report + '\n'

def write_campaigns_to_report(df, head=0):
    append_report('| Campaign | PhEDEx Size (PB) | DBS Size (PB) | Ratio | MSS Size (PB) | Second MSS Size (PB) |')
    append_report('| ------- | ------ | ------ | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['campaign'] + 
                      ' | ' + str(round(row['total_phedex_size'], 1)) + 
                      ' | ' + str(round(row['total_dbs_size'], 1)) + 
                      ' | ' + '{:.6f}'.format(float(row['total_phedex_size']/row['total_dbs_size'])) + 
                      ' | ' + str(round(row['SiteA'], 1)) + 
                      ' | ' + str(round(row['SiteB'], 1)) + 
                      ' |')

def write_sites_to_report(df, head=0):
    append_report('| Site | Campaign Count |')
    append_report('| ------- | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + index + ' | '+ str(int(row['campaign_count'])) + ' |')

def copy_directory(src, dest):
    dest_dir = os.path.dirname(dest)
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

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
    if not os.path.exists(PLOTS_PATH):
        os.makedirs(PLOTS_PATH)

def read_report_template():
    global report
    with open('../campaign_aggregation_template.md') as f:
        report = f.read()

def write_report():
    global report
    with open('../CERNTasks.wiki/CMS_Campaign_Reports.md', 'w') as f:
        f.write(report)

def commit_report():
    os.system('(cd ../CERNTasks.wiki/; git add -A; git commit -m "Auto-commiting report"; git push origin master)')

def get_second_max(columns, row):
    list = row[columns].dropna().sort_values()
    return list[len(list) - 2] if len(list) > 1 else float('nan')

def analyse_data_by_campaign():
    df = pd.read_csv('campaigns_df.csv')

    append_report('## Campaigns')

    append_report('In this table MSS means *Most Significant Site*. This is the site that contains the most amount of that campaigns data.')

    # result = df.groupby('campaign')\
    #            .agg({'dbs_size': 'sum', 'phedex_size': 'sum', 'campaign': 'count'})

    result = pivot_table(df, values='phedex_size', index='campaign', columns='site', aggfunc='sum')

    site_columns = result.columns.values
    
    result['SiteA'] = result[site_columns].max(axis=1)
    result['SiteB'] = result.apply(lambda x: get_second_max(site_columns, x), axis=1)

    result = result.drop(site_columns, axis=1)

    total_sizes_df = df.groupby('campaign', as_index=False)\
                        .agg({'phedex_size': 'sum', 'dbs_size': 'sum'})\
                        .rename(columns={'phedex_size': 'total_phedex_size', 'dbs_size': 'total_dbs_size'})

    result = total_sizes_df.join(result, on='campaign')

    # result.rename(columns={'campaign': 'count'}, inplace=True)
    result.sort_values('total_dbs_size', ascending=False, inplace=True)
    result.reset_index(inplace=True)
    
    # Bytes to petabytes
    result['total_dbs_size'] = result['total_dbs_size'] / 1000000000000000
    result['total_phedex_size'] = result['total_phedex_size'] / 1000000000000000
    result['SiteA'] = result['SiteA'] / 1000000000000000
    result['SiteB'] = result['SiteB'] / 1000000000000000
    
    append_report('### Showing TOP 10 most significant campaigns')
    write_campaigns_to_report(result, 10)

    append_report('#### Total number of campaigns %s' % len(result.index))

def analyse_data_by_site():
    df = pd.read_csv('campaigns_df.csv')

    append_report('## Sites')

    result = df.groupby('site')\
               .agg({'campaign': 'count'})

    result.rename(columns={'campaign': 'campaign_count'}, inplace=True)
    result.sort_values('campaign_count', ascending=False, inplace=True)

    append_report('### Showing TOP 10 most significant sites')
    write_sites_to_report(result, 10)

    append_report('#### Total number of sites %s' % len(result.index))

    # Plot
    result.plot(kind='bar', subplots=False, layout=(2,1), figsize=(20, 6), fontsize=6)

    plt.xticks(rotation=45, horizontalalignment='right')
    plt.tight_layout()
    plot_filename = PLOTS_PATH + 'site_count_plot.jpg'
    plt.savefig(plot_filename, dpi=120)

    append_report('### Plot')
    append_report('[[images/' + plot_filename + ']]')

    # Move plot files to wiki repo
    copy_directory(PLOTS_PATH, '../CERNTasks.wiki/images/' + PLOTS_PATH)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true",
                        dest="commit", 
                        default=False, 
                        help="Determines whether report should be committed to Github wiki")
    opts = parser.parse_args()

    create_plot_dirs()
    read_report_template()
  
    analyse_data_by_campaign()
    analyse_data_by_site()

    write_report()

    if opts.commit == True:
        commit_report()

if __name__ == '__main__':
    main()