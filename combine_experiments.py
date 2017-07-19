"""
Stage 1 in recomputing packet loss

input: 
	directory to find files
	each file is a 15 minute experiment organized by increasing time

output:
	a new set of files that are organized by target/dst pairs 
	each line represents an experiment for this pair


mechanics:
	This script uses a dictionary from target/dst --> filehandle
	When we encounter a line we check if the target/dst pair has been seen before
	if it has, get the filehandle and append to that file
	otherwise create a file handle and write the line 
	everything goes into the specified output directory
        AGG edit: original mechanism opened too many files at once causing script to crash.
        Sidetracking key-value mechanism and just opening and closing each individual file 
        when needed.
"""

import os
import json
import sys

input_dir = sys.argv[1]
output_dir = sys.argv[2]

files = os.listdir(input_dir)
files.sort()
files = map(lambda x: input_dir + x, files)
fhandle_dict = dict() #a dictionary to tell us if we already have an open file handle


#to determine if all the responses are from the same ip address
def all_from_same(responses):
    s = set()
    from_field_list = map(lambda x: x['from'], responses)
    s = reduce(append_set, from_field_list, s)
    return len(s) is 1

#grab the following fields and create a key value pair
def line_to_kv(line):
    from_ip = line['responses'][0]['from']
    dst_ip = line['dst']
    responses = line['responses']
    key = (from_ip, dst_ip)
    val = (responses)
    return (key, val)

def append_set(s, elem):
    s.add(elem)
    return s



count = 0

#take each file and grab the target, dst ip 
#if that we have seen that pair write the data to a file
#else create a file and write to that file
for f in files:
	count += 1
	print str(count) + '/' + str(len(files))
        input_file = open(f)
	d = input_file.readlines()
	j = map(json.loads, d)

	for line in j:
		if not all_from_same(line['responses']):
			continue

		key, val = line_to_kv(line)
		#if key in fhandle_dict:
		#	fhandle = fhandle_dict[key]
		#	json.dump(line, fhandle)
		#	fhandle.write('\n')
		#else:
		fhandle = open(output_dir + key[0] + '_' + key[1], 'w+')
		json.dump(line, fhandle)
		fhandle.write('\n')
		#fhandle_dict[key] = fhandle
        fhandle.close()
        input_file.close()
