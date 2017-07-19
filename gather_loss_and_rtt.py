import sys
import os


def maybe_zero_pad(string):
	if len(string) != 2:
		return "0" + string
	return string

if len(sys.argv) != 7:
	print "Usage: python gather_loss_and_rtt.py [monitor] [path_to_as_file] [start_day] [end_day] [month] [year]"
	exit()

monitor = sys.argv[1]
path_to_as_file = sys.argv[2]
start_day = maybe_zero_pad(sys.argv[3])
end_day = maybe_zero_pad(sys.argv[4])
month = maybe_zero_pad(sys.argv[5])
year = sys.argv[6]


start_date = month + "_" + start_day + "_" + year
end_date = month + "_" + end_day + "_" + year

print "Gathering packet loss and rtt data for monitor", monitor, "from", start_date, "to", end_date
cmd_rtt = ["python gather_packet_rtt.py", monitor, path_to_as_file, start_date, end_date]
cmd_loss = ["python gather_packet_loss.py", start_day, end_day, month, year, monitor]

cmd_rtt = " ".join(cmd_rtt)
cmd_loss = " ".join(cmd_loss)
os.system(cmd_rtt)
os.system(cmd_loss)
