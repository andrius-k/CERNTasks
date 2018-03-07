import pandas as pd

import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt
from subprocess import check_output
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
    append_report('| Campaign | PhEDEx Size (PB) | DBS Size (PB) | Ratio | Most Significant Site | Second Most Significant Site | Most Significant Site Size (PB) | Second Most Significant Site Size (PB) | Number of Sites |')
    append_report('| ------- | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['campaign'] + 
                      ' | ' + str(round(row['phedex_size'], 1)) + 
                      ' | ' + str(round(row['dbs_size'], 1)) + 
                      ' | ' + '{:.2f}'.format(float(row['phedex_size']/row['dbs_size'])) + 
                      ' | ' + row['mss_name'] + 
                      ' | ' + row['second_mss_name'] + 
                      ' | ' + str(round(row['mss'], 1)) + 
                      ' | ' + str(round(row['second_mss'], 1)) + 
                      ' | ' + str(row['sites']) + 
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
                      ' | ' + '{:.2f}'.format(float(row['phedex_size']/row['dbs_size'])) + 
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
    for i, (idx, row) in enumerate(head.set_index('campaign').drop(['mss_name', 'second_mss_name'], axis=1).iterrows()):
        ax = axes[i // 3, i % 3]
        row = row[row.gt(row.sum() * .01)]
        ax.pie(row, labels=row.index, startangle=30)
        ax.set_title(idx)
    
    plt.tight_layout()
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1, wspace=0.4)

    plot_filepath = PLOTS_PATH + file_name
    plt.savefig(plot_filepath, dpi=120)

    return plot_filepath

def analyse_data_by_campaign():
    df = pd.read_csv('campaigns_dbs_df.csv')

    append_report('## Campaigns')

    # Bytes to petabytes
    df['dbs_size'] = df['dbs_size'] / 1000000000000000
    df['phedex_size'] = df['phedex_size'] / 1000000000000000
    df['mss'] = df['mss'] / 1000000000000000
    df['second_mss'] = df['second_mss'] / 1000000000000000
    
    append_report('### Showing TOP 10 most significant campaigns by DBS size')
    write_campaigns_to_report(df, 10)

    # Make pie chart of sites for most significant DBS campaigns
    plot_filepath = plot_pie_charts(df, 'dbs_size_campaigns_plot.jpg')

    append_report('### Plot of 6 most significant DBS campaigns')
    append_report('Each pie chart visualises the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant DBS campaigns](images/%s)' % plot_filepath)

    df = pd.read_csv('campaigns_phedex_df.csv')

    # Bytes to petabytes
    df['dbs_size'] = df['dbs_size'] / 1000000000000000
    df['phedex_size'] = df['phedex_size'] / 1000000000000000
    df['mss'] = df['mss'] / 1000000000000000
    df['second_mss'] = df['second_mss'] / 1000000000000000

    append_report('### Showing TOP 10 most significant campaigns by PhEDEx size')
    write_campaigns_to_report(df, 10)

    # Make pie chart of sites for most significant PhEDEx campaigns
    plot_filepath = plot_pie_charts(df, 'phedex_size_campaigns_plot.jpg')

    append_report('### Plot of 6 most significant PhEDEx campaigns')
    append_report('Each pie chart visualises the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant PhEDEx campaigns](images/%s)' % plot_filepath)

def analyse_campaign_tier_relationship():
    df = pd.read_csv('campaign_tier_df.csv')

    append_report('## Campaign sizes in data tiers')

    append_report('### Showing TOP 20 most significant campaign - tier pairs')
            
    # Bytes to petabytes
    df['dbs_size'] = df['dbs_size'] / 1000000000000000 
    df['phedex_size'] = df['phedex_size'] / 1000000000000000 

    write_campaign_tier_relationship_to_report(df, 20)

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
    analyse_campaign_tier_relationship()

    write_report()

    if opts.commit == True:
        commit_report()

if __name__ == '__main__':
    main()
