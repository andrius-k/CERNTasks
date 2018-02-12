# Task 2

import pandas as pd

import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt
import os, shutil, subprocess

PHEDEX_PLOTS_PATH = 'phedex_plots/'
DBS_PLOTS_PATH = 'dbs_plots/'

report = ''

def append_report(lines):
    global report
    report = report + lines
    report = report + '\n'

def write_df_to_report(df):
    append_report('| Tier | Tier count | Size (PB) |')
    append_report('| ------- | ------ | ------ |')

    for index, row in df.iterrows():
        append_report(' | ' + index + ' | ' + str(row['tier_count']) + ' | ' + str(row['sum_size']) + ' | ' )

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
    axes = result.plot(kind='bar', subplots=True, layout=(2,1), figsize=(8, 6), fontsize=8)

    axes[0][0].set_title('')
    axes[1][0].set_title('')
    axes[0][0].set_ylabel('Number')
    axes[1][0].set_ylabel('Petabytes')

    plt.tight_layout()
    plot_filename = file_path + '_plot.jpg'
    plt.savefig(plot_filename, dpi=120)

    return plot_filename

def analyse_phedex_data():
    df = pd.read_csv('phedex_df.csv')
    sites = df.groupby(df.site.str[:2])['site'].agg(lambda x: set(x)).index.tolist()

    append_report('## PhEDEx data')

    for site in sites:
        result = df[df.site.str[:2] == site] \
            .groupby(df.dataset.str.split('/').str[3]) \
            .agg({'size': 'sum', 'site': 'count'})

        # Bytes to petabytes
        result['size'] = result['size'] / 1000000000000000

        result = result.rename(columns={'size': 'sum_size', 'site': 'tier_count'})

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

    # Bytes to petabytes
    result['size'] = result['size'] / 1000000000000000

    result = result.rename(columns={'size': 'sum_size', 'dataset': 'tier_count'})

    append_report('## DBS data')
    write_df_to_report(result)

    print 'DBS data:'
    print(result)

    plot_filename = make_plot(result, DBS_PLOTS_PATH + 'dbs')

    append_report('### Plot')
    append_report('[[images/' + plot_filename + ']]')

    # Move plot file to wiki repo
    copy_directory(DBS_PLOTS_PATH, '../CERNTasks.wiki/images/' + DBS_PLOTS_PATH)

def main():
    create_plot_dirs()
    read_report_template()
    
    analyse_phedex_data()
    analyse_dbs_data()

    write_report()
    commit_report()

if __name__ == '__main__':
    main()
    