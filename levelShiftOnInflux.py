# Runs levelshift on all items in 
# /home/freeman/influxDB/timeSeriesAndOutputFiles

# Include
import os
import pdb
import sys
from multiprocessing import Pool

if len(sys.argv) != 2:
    print "Requires one arg, timeseries file directory"

# Global
tsFilesDir = sys.argv[1]
levelshiftExecutable = '/project/comcast-ping/kabir-plots/loss_data/levelshift'


# Runs os.system() on parameter
def systemCall(parameter):
	os.system(parameter)

# Main
myPool = Pool(20)
itemsInDir = os.listdir(tsFilesDir)
commandsList = []
print 'Running levelshift on ts files...'
for ts in itemsInDir:
	if '.out' in ts or '.png' in ts or '.py' in ts:
		continue
	command = 'cat ' + tsFilesDir + ts + ' | ' + levelshiftExecutable + \
' -L 12 > ' + tsFilesDir + ts + '.out'
	commandsList.append(command)

myPool.map(systemCall, commandsList)
#print 'Done. Running levelshift_plotter_influxdb.py on .ts and .out files...'

itemsInDir = os.listdir(tsFilesDir)
commandsList = []
# Now run levelshift_plotter_influxdb.py on all .ts files. format:
# python levelshift_plotter_influxdb.py "file.ts file.out"
"""
for ts in itemsInDir:
	if '.out' in ts or '.png' in ts or '.py' in ts:
		continue
	if os.stat(tsFilesDir + ts + '.out').st_size == 0 or \
os.stat(tsFilesDir + ts).st_size == 0:
		# Ignore this ts file if the .out file or .ts file is empty
		continue
	command = 'python /home/freeman/influxDB/levelshift_plotter_influxdb_newaxes.py "' \
+ tsFilesDir + ts + ' ' + tsFilesDir + ts + '.out"'
	commandsList.append(command)

myPool.map(systemCall, commandsList)

print 'Done. Collecting images and moving to Cider...'
os.system('python collectImagesForCider_ag.py')
"""
