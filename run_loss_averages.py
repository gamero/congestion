#usage python run_loss_averages.py >> supplemental_data/multiprocessing_loss_log.txt 2>&1
#gets list of ipmap files to process from hardcoded list below
#list created by running:
#ls rtt_and_loss_data/rtt/book_keeping/*/*/*ipmap* > supplemental_data/list_of_ipmaps.txt

import subprocess
from multiprocessing import Pool
import os
import signal
import time
import csv
import sys

influx_command = "python automated_loss_averages.py "
log = " >> "
log_post = " 2>&1 "
timeout_seconds = 1800

ipmap_file = "/project/comcast-ping/kabir-plots/loss_data/supplemental_data/list_of_ipmaps.txt"
#one hour

def systemCall(parameter):
    os.system(parameter)

def create_commands(input_file):
#Create list of commands to pass to the multiprocessing manager
    #open grab monitor and time-period from IP map filename
    commandsList = []
    monitor = input_file.split('/')[-1].split('.')[0]

    time_period_year = input_file.split('/')[-2].split('_')[-1]
    year_string = time_period_year + '_'
    year_string_replace = time_period_year + '.'
    time_period = input_file.split('/')[-2].replace(year_string, year_string_replace)

    rtt_path = input_file.replace('book_keeping','levelshift')
    loss_path = input_file.replace('/rtt/','/loss/').replace('book_keeping/','')

    #remove filename portion
    replacing_path = input_file.split('/')[-1]
    rtt_path = rtt_path.replace(replacing_path,'')
    loss_path = loss_path.replace(replacing_path,'')

    #Then, grab from each line the ASN, near-end IP and far-end IP
    #Use that to build command string for final heuristics
    with open (input_file,'rb') as f:
        reader = csv.reader(f, delimiter=' ')
        for row in reader:
            asn = row[0]
            near_ip = row[1]
            far_ip = row[2]

            far_filename = rtt_path + monitor + '.' + asn + '.' + time_period + '.' + far_ip + '.ts'
            near_filename = rtt_path + monitor + '.' + asn + '.' + time_period + '.' + near_ip + '.ts'
            far_window = far_filename + '.win.txt'

            far_loss = loss_path + far_ip + '.ts'
            near_loss = loss_path + near_ip + '.ts'

            log_file = far_loss + '.log'          

            command = influx_command + far_filename + ' ' + near_filename + ' ' + \
                    far_loss + ' ' + near_loss + ' ' + far_window + log + log_file + log_post
            
            commandsList.append(command)

    return commandsList	

def read_ipmap(filename):
    #read ipmap lines into list
    ipmap_list = ''
    with open(filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
    return ipmap_list

def run_commands(command_list):

	myPool = Pool(20)
	print "running following commands"
	for i in range(len(command_list)):
		print command_list[i]
#	crawl_withtimeout(command_list[0])
	myPool.map_async(systemCall, command_list)
	myPool.close()
	myPool.join()

def print_commands(command_list):
    for i in range(len(command_list)):
        print command_list[i]

def call_create_commands(ipmap_list):
    
    combined_list = []
    temp_array = []
    for i in range(len(ipmap_list)):
        passing_string = ipmap_list[i].strip('\n')
        temp_array = create_commands(passing_string)
        combined_list = combined_list + temp_array
    return combined_list

def main():

    command_list = []

    #get list of ipmap files using below list
    ipmap_list = read_ipmap(ipmap_file)

    #create influx queries for each monitor-month tuple
    command_list = call_create_commands(ipmap_list)

    #print commands
    #print_commands(command_list)
   
    #run influx/loss commands 20 at a time
    run_commands(command_list)
		
main()
