"""
You can invoke this file directly, but you be using gather_loss_and_rtt.py

You should only touch this file if you need to coordinate more steps in packet loss
or you want to change the way packet loss data is being uncompressed

This file takes in arguments, uncompresses packet loss data, and coordinates
the rest of packet loss through combine_experiments.py, compute_new_loss.py, and
combine_froms.py

input:
	uncompress.py [start_day] [end_day] [month] [year] [monitor]

output:
	output is the packet loss data ready for analysis 
"""


import sys
import os
from multiprocessing import Pool


if len(sys.argv) != 6:
	print "uncompress.py [start_day] [end_day] [month] [year] [monitor]"
	exit()

def maybe_zero_pad(string):
	if len(string) != 2:
		return "0" + string
	return string


def process_args():
	start_day = sys.argv[1]
	end_day = sys.argv[2]
	month = sys.argv[3]
	year = sys.argv[4]
	monitor = sys.argv[5]

	start_day = maybe_zero_pad(start_day)
	end_day = maybe_zero_pad(end_day)
	month = maybe_zero_pad(month)

	if int(start_day) > int(end_day):
		print "start day must be after end day"
		print "uncompress.py [start_day] [end_day] [month] [year] [monitor]"
		exit()

	return start_day, end_day, month, year, monitor

def compressed_loss_data_directory(start_day, end_day, month, year, monitor):
	dirname = '/project/comcast-ping/tsp-loss/' + monitor + "/" + year + "/" + month + "/"
        try:
            temp_list = os.listdir(dirname)
        except OSError:
            temp_list = ''
	return dirname, temp_list

def in_between(start, end, curr):
	curr_date = curr.split(".")[2]
	return start <= curr_date and end >= curr_date

def get_uncompression_command(fullname, output_dir, shortname):
	return "zcat " + fullname + " | /home/amogh/software/scamper/bin/sc_warts2json > " + output_dir + shortname 

def main():


	start_day, end_day, month, year, monitor = process_args()
	loss_data_dir, all_files = compressed_loss_data_directory(start_day, end_day, month, year, monitor)
        if len(all_files) is 0:
            print "There is no packet loss data"
            exit()
	start_date = "".join([year, month, start_day])
	end_date = "".join([year, month, end_day])

	step_zero_dir = "_".join(["tmp_data/uncompressed_loss", start_date, end_date, monitor]) + "/"
	step_one_dir = "_".join(["tmp_data/combined_experiments_data", start_date, end_date, monitor]) + "/"
	step_two_dir = "_".join(["tmp_data/combined_from_dst_data", start_date, end_date, monitor]) + "/"
	step_three_dir = "_".join(["tmp_data/final_loss_data", start_date, end_date, monitor]) + "/"

	os.system("mkdir -p " + step_zero_dir)

	#get files for uncompression
	all_files = filter(lambda curr: in_between(start_date, end_date, curr), all_files)
	all_files.sort()

	if len(all_files) is 0:
		print "There is no packet loss data"
		exit()

	all_commands = []
	for f in all_files:
		all_commands.append(get_uncompression_command(loss_data_dir + f, step_zero_dir, f))

	pool = Pool(processes = 8)
	print ("Uncompressing json loss data")
	pool.map(os.system, all_commands)

	os.system("rm -rf " + step_one_dir)
	os.system("rm -rf " + step_two_dir)
	os.system("rm -rf " + step_three_dir)

	os.system("mkdir -p " + step_one_dir)
	os.system("mkdir -p " + step_two_dir)
	os.system("mkdir -p " + step_three_dir)
	os.system('mkdir -p rtt_and_loss_data/loss')

	print "Combining experiments (this will be slow)"
	os.system("python combine_experiments.py " + step_zero_dir + " " + step_one_dir)
	print "Computing new loss rates"
	os.system("python compute_new_loss.py " + step_one_dir + " " + step_two_dir)
	print "Combining new loss data"
	os.system("python combine_froms.py " + step_two_dir + " " + step_three_dir)

	start_date = "_".join([month, start_day, year])
	end_date = "_".join([month, end_day, year])
	
	final_loss_dir = "rtt_and_loss_data/loss/" + monitor + "/" + start_date + "_" + end_date + "/"
	os.system("mkdir -p " + final_loss_dir)
	print "Moving loss data to final directory"
	os.system("mv " + step_three_dir + "* " + final_loss_dir)

	os.system("rm -rf " + step_zero_dir)
	os.system("rm -rf " + step_one_dir)
	os.system("rm -rf " + step_two_dir)
	os.system("rm -rf " + step_three_dir)

main()

