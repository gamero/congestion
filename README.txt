The files in this directory are responsible for gathering packet loss and rtt
data for a given time period. Here is a summary of each file

final_heuristics.py (agamerog)

Filters levelshift heuristics and computes congestion intervals

gather_loss_and_rtt.py (kgogia)

	To use this script, call it like this. 

	python gather_loss_and_rtt.py [monitor] [path_to_as_file] [start_day] [end_day] [month] [year]
	python gather_loss_and_rtt.py bed-us /path/to/as/file 3 4 3 2017

	This will get packet loss and rtt data for bed-us for march 3rd-4th 2017
	This script will invoke two scripts, gather_packet_rtt and gather_packet_loss

gather_packet_loss.py (kgogia / agamerog)

This file is responsible for uncompressing json loss data, recomputing it in
5 minute bins, and outputing the loss data. To do this, it invokes three scripts,
combine_experiments, combine_froms, compute_new_loss. The output data will be in

rtt_and_loss_data/loss/[monitor]/[date_range]

This script uses tmd_data/ directory for intermediate data when recomputing loss rate

gather_packet_rtt.py (kgogia / agamerog)

This file is responsible for querying influx and gather rtt data for the 
specified time range. It outputs quite a bit of data

rtt_and_loss_data/rtt/levelshift/[monitor]/[date_range]

This is the timeseries data and levelshift output

rtt_and_loss_data/rtt/book_keeping/[monitor]/[date_range]

contains book_keeping data such as timeseries data per asn and the links
for each asn

combine_experiments.py (kgogia)

This script is responsible for taking the json loss data and combining experiments
by their from/dst ip pair. This is the first step in computing loss rate.

compute_new_loss.py (kgogia)

This script is responsible for taking the output of combine_experiments.py and
recomputing loss rate in 5 minute buckets

combine_froms.py (kgogia / agamerog)

This script is responsible for taking th eoutput of compute_new_loss.py and
combining the from/dst pairs into a single data point using the mean

levelshift

The levelshift executable

levelShiftOnInflux.py (kfreeman / agamerog)

This script takes in a single argument which is the path to the directory that 
it will compute levelshift on

old_data

This is the data that I gathered for packet loss for various monitors over a couple months.
I kept it around in case it is useful, but it does not follow the format we use now so I'd recommend
that this be thrown away and the data be recomputed. 

rtt_and_loss_data
This is the output directory and where all relevant data will be placed

tmp_data
This directory is used for temporary data when recomputing loss data. This keeps
the project root clean and all temp data contained. 