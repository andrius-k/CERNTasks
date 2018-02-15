from CMSSpark.phedex import run
import time, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--fout", action="store", dest="fout", help='Output file name')
parser.add_argument("--date", action="store", dest="date", default="", help='Select CMSSW data for specific date (YYYYMMDD)')
parser.add_argument("--yarn", action="store_true", dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
parser.add_argument("--verbose", action="store_true", dest="verbose", default=False, help="verbose output")
opts = parser.parse_args()

fout = opts.fout
date = opts.date
yarn = opts.yarn
verbose = opts.verbose

start = time.time()
run(date, fout, yarn, verbose)
end = time.time()

with open('phedex_time_data.txt', 'w') as file: 
    file.write(str(end - start))

