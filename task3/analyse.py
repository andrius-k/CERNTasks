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
    append_report('| Campaign | PhEDEx Size (PB) | DBS Size (PB) | Ratio | Most Significant Site | Second Most Significant Site | Most Significant Site Size (PB) | Second Most Significant Site Size (PB) |')
    append_report('| ------- | ------ | ------ | ------ | ------ | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['campaign'] + 
                      ' | ' + str(round(row['total_phedex_size'], 1)) + 
                      ' | ' + str(round(row['total_dbs_size'], 1)) + 
                      ' | ' + '{:.2f}'.format(float(row['total_phedex_size']/row['total_dbs_size'])) + 
                      ' | ' + row['mss'] + 
                      ' | ' + row['second_mss'] + 
                      ' | ' + str(round(row['mss_value'], 1)) + 
                      ' | ' + str(round(row['second_mss_value'], 1)) + 
                      ' |')
        
def write_campaign_tier_relationship_to_report(df, head=0):
    append_report('| Campaign | Tier | DBS Size (PB) | PhEDEx Size (PB) | Ratio |')
    append_report('| ------- | ------ | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['campaign'] + 
                      ' | ' + row['tier'] + 
                      ' | ' + str(round(row['dbs_size'], 1)) + 
                      ' | ' + str(round(row['phedex_size'], 1)) + 
                      ' | ' + '{:.6f}'.format(float(row['phedex_size']/row['dbs_size'])) + 
                      ' |')

def write_sites_to_report(df, head=0):
    append_report('| Site | Campaign Count |')
    append_report('| ------- | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + index + 
                      ' | ' + str(int(row['campaign_count'])) + 
                      ' |')

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

def append_report_header():
    append_report('# PhEDEx and DBS data aggregation based on campaigns for data from 2017-02-28')
    append_report('Results of gathering PhEDEx and DBS information aggregated by campaign')

def write_report():
    global report
    with open('../CERNTasks.wiki/CMS_Campaign_Reports.md', 'w') as f:
        f.write(report)

def commit_report():
    os.system('(cd ../CERNTasks.wiki/; git add -A; git commit -m "Auto-commiting report"; git push origin master)')

def plot_pie_charts(df, file_name):
    head = df.head(6)

    fig, axes = plt.subplots(2, 3, figsize=(30, 15))
    for i, (idx, row) in enumerate(head.set_index('campaign').drop(['mss', 'second_mss'], axis=1).iterrows()):
        ax = axes[i // 3, i % 3]
        row = row[row.gt(row.sum() * .01)]
        ax.pie(row, labels=row.index, startangle=30)
        ax.set_title(idx)
    
    plt.tight_layout()
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1, wspace=0.4)

    plot_filepath = PLOTS_PATH + file_name
    plt.savefig(plot_filepath, dpi=120)

    return plot_filepath

def get_second_max(columns, row):
    list = row[columns].dropna().sort_values()
    return list[len(list) - 2] if len(list) > 1 else float('nan')

def get_second_max_site(columns, row):
    list = row[columns].dropna().sort_values(ascending=False)
    if len(list) > 1:
        if row[row==list[1]].size > 1:
            return row[row==list[1]].index[1]
        else:
            return row[row==list[1]].index[0]
    else:
        return '-'

def get_max_site(columns, row):
    list = row[columns].dropna().sort_values(ascending=False)
    return row[row==list[0]].index[0]

def analyse_data_by_campaign():
    df = pd.read_csv('campaigns_df.csv')

    append_report('## Campaigns')

    result = df

    # Remove part of site after third underscore
    result['site'] = df.apply (lambda row: row['site'] if row['site'].count('_') < 3 else row['site'][:row['site'].rfind('_')], axis=1)

    result = pivot_table(df, values='phedex_size', index='campaign', columns='site', aggfunc='sum')

    site_columns = result.columns.values
    
    result['mss'] = result.apply(lambda x: get_max_site(site_columns, x), axis=1)
    result['second_mss'] = result.apply(lambda x: get_second_max_site(site_columns, x), axis=1)
    result['mss_value'] = result[site_columns].max(axis=1)
    result['second_mss_value'] = result.apply(lambda x: get_second_max(site_columns, x), axis=1)

    # When aggregating dbs_size take just first value instead of sum so avoid duplication.
    # Each DBS size with the same campaign is a duplicate!
    total_sizes_df = df.groupby('campaign', as_index=False)\
                        .agg({'phedex_size': 'sum', 'dbs_size': 'first'})\
                        .rename(columns={'phedex_size': 'total_phedex_size', 'dbs_size': 'total_dbs_size'})

    result = total_sizes_df.join(result, on='campaign')

    result.sort_values('total_dbs_size', ascending=False, inplace=True)
    result.reset_index(inplace=True)
    
    # Bytes to petabytes
    result['total_dbs_size'] = result['total_dbs_size'] / 1000000000000000
    result['total_phedex_size'] = result['total_phedex_size'] / 1000000000000000
    result['mss_value'] = result['mss_value'] / 1000000000000000
    result['second_mss_value'] = result['second_mss_value'] / 1000000000000000
    
    append_report('### Showing TOP 10 most significant campaigns by DBS size')
    write_campaigns_to_report(result, 10)

    # Make pie chart of sites for most significant DBS campaigns
    plot_filepath = plot_pie_charts(result, 'dbs_size_campaigns_plot.jpg')

    append_report('### Plot of 6 most significant DBS campaigns')
    append_report('Each pie chart visualises the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant DBS campaigns](images/%s)' % plot_filepath)

    result.sort_values('total_phedex_size', ascending=False, inplace=True)

    append_report('### Showing TOP 10 most significant campaigns by PhEDEx size')
    write_campaigns_to_report(result, 10)

    # Make pie chart of sites for most significant PhEDEx campaigns
    plot_filepath = plot_pie_charts(result, 'phedex_size_campaigns_plot.jpg')

    append_report('### Plot of 6 most significant PhEDEx campaigns')
    append_report('Each pie chart visualises the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant PhEDEx campaigns](images/%s)' % plot_filepath)

    append_report('#### Total DBS data size: %d PB' % result['total_dbs_size'].sum())
    append_report('#### Total PhEDEx data size: %d PB' % result['total_phedex_size'].sum())
    append_report('#### Total number of campaigns %s' % len(result.index))

def analyse_data_by_site():
    df = pd.read_csv('campaigns_df.csv')

    append_report('## Sites')

    result = df

    # Remove part of site after third underscore
    result['site'] = df.apply (lambda row: row['site'] if row['site'].count('_') < 3 else row['site'][:row['site'].rfind('_')], axis=1)

    result = df.groupby('site')\
               .agg({'campaign': 'nunique'})

    result.rename(columns={'campaign': 'campaign_count'}, inplace=True)
    result.sort_values('campaign_count', ascending=False, inplace=True)

    append_report('### Showing TOP 10 most significant sites by campaign count')
    write_sites_to_report(result, 10)

    append_report('#### Total number of sites %s' % len(result.index))

    # Plot
    result.plot(kind='bar', subplots=False, layout=(2,1), figsize=(20, 6), fontsize=6)

    plt.xticks(rotation=45, horizontalalignment='right')
    plt.tight_layout()
    plot_filepath = PLOTS_PATH + 'site_count_plot.jpg'
    plt.savefig(plot_filepath, dpi=120)

    append_report('### Plot')
    append_report('![Site count](images/' + plot_filepath + ')')

    # Move plot files to wiki repo
    copy_directory(PLOTS_PATH, '../CERNTasks.wiki/images/' + PLOTS_PATH)

def analyse_campaign_tier_relationship():
    df = pd.read_csv('campaign_tier_df.csv')

    append_report('## Campaign sizes in data tiers')

    append_report('### Showing TOP 10 most significant campaign - tier pairs')

    result = df

    result['campaign'] = result['dataset'].str.split('/').str[2]
    result['tier'] = result['dataset'].str.split('/').str[3]

    result = result.groupby(['campaign', 'tier'])\
                   .agg({'dbs_size': 'sum', 'phedex_size': 'sum'})\
                   .sort_values(by='dbs_size', ascending=False)\
                   .reset_index()
            
    # Bytes to petabytes
    result['dbs_size'] = result['dbs_size'] / 1000000000000000 
    result['phedex_size'] = result['phedex_size'] / 1000000000000000 

    write_campaign_tier_relationship_to_report(result, 20)

    total_dbs_size = result['dbs_size'].sum()
    total_phedex_size = result['phedex_size'].sum()

    append_report('#### Total number of campaign - tier pairs %s' % len(result.index))
    append_report('#### Total DBS sum of sizes %d PB' % total_dbs_size)
    append_report('#### Total PhEDEx sum of sizes %d PB' % total_phedex_size)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true",
                        dest="commit", 
                        default=False, 
                        help="Determines whether report should be committed to Github wiki")
    opts = parser.parse_args()

    create_plot_dirs()
    append_report_header()
  
    analyse_data_by_campaign()
    analyse_data_by_site()
    analyse_campaign_tier_relationship()

    write_report()

    if opts.commit == True:
        commit_report()

if __name__ == '__main__':
    main()
