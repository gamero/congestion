#usage python plux_crawler.py >>log_alexa1k.txt 2>&1

import subprocess
from multiprocessing import Pool
import os
import signal
import time

websites_file = '/project/comcast-ping/plots-agamerog/'
influx_command = "python gather_loss_and_rtt.py "
as_files = "/project/comcast-ping/plots-agamerog/monitor/peersproviders.txt "
#log = " >> log_influx.txt 2>&1 "
log = ''
timeout_seconds = 43200 #twelve hours

monitors = ['atl2-us',\
    'avl-us',\
    'aza-us',\
    'bed2-us',\
    'bed3-us',\
    'bed-us',\
    'bos2-us',\
    'bos5-us',\
    'cld2-us',\
    'dca2-us',\
    'gai-us',\
    'ith-us',\
    'lex-us',\
    'mnz-us',\
    'mry-us',\
    'msy-us',\
    'oak3-us',\
    'oak5-us',\
    'pao-us',\
    'san2-us',\
    'san4-us',\
    'san6-us',\
    'san7-us',\
    'tul2-us',\
    'tul-us',\
    'wbu2-us']

dates = ['1 31 3 2017',\
    '1 30 4 2017',\
    '1 31 5 2017',\
    '1 30 6 2017']

def create_commands(monitors, dates):
#Create list of commands to pass to the multiprocessing manager

    commandsList = []
    for i in range(len(monitors)):
        MON = monitors[i]
        for j in range(len(dates)):
            AS = as_files.replace('monitor',MON)
            command = influx_command + MON + ' ' + AS + dates[j] + log
            commandsList.append(command)
    return commandsList	

def read_websites(filename):

	f = open(filename, 'rb') #import file
	website_list = f.readlines()
	return website_list

def run_commands(command_list):

	myPool = Pool(20)
	print "running following commands"
	for i in range(len(command_list)):
		print command_list[i]
#	crawl_withtimeout(command_list[0])
	myPool.map_async(crawl_withtimeout, command_list)
	myPool.close()
	myPool.join()

def crawl_withtimeout(command):

	#Run command and return when either command finishes
	#Or timeout 
	start = time.time()
	print command
	pro = subprocess.Popen(command, stdout=subprocess.PIPE, \
                               shell=True, preexec_fn=os.setsid)
	while True:

		current = time.time()
		
		return_code = pro.returncode

		elapsed = current - start
		if str(return_code) != "None":
			print command + "influx finished, killing tree"
			os.killpg(os.getpgid(pro.pid), signal.SIGTERM)
			return
		if int(elapsed) >= timeout_seconds:
			print "timeout -- influx took over 12 hours" + command
			os.killpg(os.getpgid(pro.pid), signal.SIGTERM)
			return
		time.sleep(2)

def main():

    command_list = []

    #get list of monitors from file -- hardcoded monitor list for now
    #website_list = read_websites(websites_file)

    #create influx queries for each monitor-month tuple
    command_list = create_commands(monitors, dates)
 
   #run influx/loss commands 20 at a time
    run_commands(command_list)
		
main()
