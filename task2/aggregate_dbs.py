from CMSSpark.dbs_events import run
import time, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--fout", action="store", dest="fout", help='Output file name')
parser.add_argument("--yarn", action="store_true", dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
parser.add_argument("--verbose", action="store_true", dest="verbose", default=False, help="verbose output")
opts = parser.parse_args()

fout = opts.fout
yarn = opts.yarn
verbose = opts.verbose

start = time.time()
run(fout, yarn, verbose)
end = time.time()

with open('dbs_time_data.txt', 'w') as file: 
    file.write(str(end - start))

