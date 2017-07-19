"""
input:
	This previous steps use from/dst pairs, so there are likely files
	with the same froms, but different dst. We want to merge this into a single
	file so data points at the same time can be averaged. 

output:
	Data that is ready for analysis. Output is in the standard time series format
	timedata [space] value

"""

import os
import pickle
from multiprocessing import Pool
import sys

input_dir = sys.argv[1]
output_dir = sys.argv[2]


def get_from_dict(d, k):
	return d[k] if k in d else []

def append_to_dict(d, k, v):
	curr = get_from_dict(d, k)
	curr.append(v)
	d[k] = curr
	return d 

#take a value which is an array and insert or append to the dict
def append_and_flatten(d, k, v):
    curr = get_from_dict(d, k)
    curr = curr + v
    d[k] = curr

#take two dictionaries and return the union
def merge_dicts(d1, d2):
    d3 = dict()

    for key in d1.keys():
        d3[key] = d1[key]

    for key in d2.keys():
        append_and_flatten(d3, key, d2[key])
    return d3

def to_dict(l):
	return reduce(lambda acc,x: append_to_dict(acc, x[0], x[1:]), l, dict())

def combine_loss(l):
	return reduce(lambda acc, x: (acc[0] + x[0], acc[1] + x[1]), l, (0,0))

def set_dict(d, k, v):
	d[k] = v
	return d

#write the contents in levelshift format
def prepare_ls(fhandle, l):
	for ts, loss in l:
		loss_rate = (loss[0] / float(loss[1]) * 100)
		fhandle.write(str(ts) + ' ' + str(loss_rate) + '\n')

#open the input files, deserialize the lists, and combine the same froms 
def open_prep(from_ip, fnames):
        number_files = str(len(fnames))
        command = 'NUMBER OF FILES BEING OPENED = ' + number_files 
        print command
	fnames = map(lambda x: input_dir + x, fnames)
	open_files = map(lambda x: open(x, 'rb'), fnames) #open each file
	pickled_lists = map(pickle.load, open_files) #load each file
	time_to_loss = map(to_dict, pickled_lists) #convert each from,dst pair into a dict

	merged = reduce(merge_dicts, time_to_loss, dict()) #merge all the dicts
	merged = reduce(lambda acc, x: set_dict(acc, x[0], combine_loss(x[1])), merged.items(), dict())
	a = merged.items()
	a.sort(lambda x, y: x[0] - y[0])
	map(lambda x: x.close(), open_files)
	fhandle = open(output_dir + from_ip + '.ts', 'w+')
	prepare_ls(fhandle, a)
	fhandle.close()

files = os.listdir(input_dir)

#split processing into four groups to avoid opening too many files at once
#first = int(len(files)/4)
print "len of files " + str(len(files))
#second = first * 2
#third = first * 3

#for i in [first,second,third,-1]
#files_first = files[:first]

from_to_file = dict()

pool = Pool(processes=4)

#from -> [file1, file2, file3...]
from_to_file = reduce(lambda acc, x: append_to_dict(acc, x.split('_')[0], x), files, dict())
map(lambda x: open_prep(x[0], x[1]), from_to_file.items()) #process each list of files


