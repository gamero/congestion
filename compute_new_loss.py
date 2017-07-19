"""
This file is responsible for computing new loss rates with 5 minute buckets for
each of the input files

input:
    each file is the output of combine_experiments.py

output:
    new files with a serialized list (using pickle) with all the experiment data
    for this target/dst pair 

mechanics:

"""


import os
import json
import pickle
import sys

from multiprocessing import Pool 


def bin_time(ts):
    return (ts/300) * 300

def get_stats(response):
    return response['seq'], response['tx']['sec']

def compute_loss_rate(responses):

    output_stats = []
    missing = 0 
    rcvd = 0

    start_seq, start_time = get_stats(responses[0])

    expected_seq = start_seq
    end_time = start_time + 300 #5 minutes in seconds
    
    for response in responses:

        curr_seq, curr_time = get_stats(response)

        #current window is finished
        if curr_time > end_time:
            output_stats.append( (bin_time(start_time), missing, rcvd+missing) ) 
            missing, rcvd, start_time, end_time = 0, 0, curr_time, curr_time + 300 #set up next window

        if curr_seq != expected_seq:
            missing += curr_seq - expected_seq #update missing to be the gap
            expected_seq = curr_seq #move expected up to the last seen 


        rcvd += 1 #increment the total we have seen so far
        expected_seq += 1 #increment the next expected value

    if rcvd != 0:
        output_stats.append( (bin_time(curr_time), missing, rcvd+missing) )
    return output_stats


#to compute loss accurately, we must have sequence numbers that increment monotoincally
def update_sequence_numbers(responses):
	offset = 0
	for experiment in responses:
		for response in experiment:
			response['seq'] += offset
		offset += 1000


input_dir = sys.argv[1]
output_dir = sys.argv[2]

#take each file and combine all the responses and compute the new loss rate
#write this new data as a python list
def convert(f):
        input_file = open(f)   
        j = map(json.loads, input_file.readlines())
        l = list()
        for i in j:
                l.append(i['responses'])
        computed_loss = list()
        update_sequence_numbers(l)
        for i in l:
                computed_loss += i

        computed_loss = compute_loss_rate(computed_loss)
        output_file_handler = open(output_dir + os.path.basename(f), 'w+')
        pickle.dump(computed_loss, output_file_handler)
        output_file_handler.close()
        input_file.close()
pool = Pool(processes=8) 
files = os.listdir(input_dir)
files.sort()
files = map(lambda x: input_dir + x, files)
pool.map(convert, files)
	



