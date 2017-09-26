#usage: python automated_loss_averages.py far_ts near_ts far_loss near_loss far_window
#input: rtt, loss, and congestion window files
#if <plotting_bool> is true, a plot is attempted
#output: average loss in congested periods and outside of it
#output is "nan" or -1 if there is no packet loss data inside the congested window
#output format is 
#monitor,asname,far_IP,avg_far_congested,avg_far_uncongested,avg_near_congested,far_diff,near_diff,diff_diff
import csv
import numpy as np
import sys
import os

plotting = True

def read_windows(filename):
    h = open(filename, 'rb+')
    starts = []
    ends = []
    reader = csv.reader(h, delimiter=' ')
    for row in reader:
        starts.append(int(row[0]))
        ends.append(int(row[1])) 
    return starts, ends  

def is_in_window(timestamp, starts, ends):
    for i in range(len(starts)):
        if timestamp <= ends[i] and timestamp >= starts[i]:
            return True
    return False

def convert_to_dot(long_IP):
    
    IP_dots = str(int(long_IP[0:3])) + '.' + str(int(long_IP[3:6])) + '.' + \
            str(int(long_IP[6:9])) + '.' + str(int(long_IP[9:12]))

    return IP_dots

def read_asnames():

    AS = open('/home/agamerog/plots/ddc/AS-table.txt', 'rU')
    ASdict = {}
    for lines in AS:
        if len(lines) > 0 and lines[0] == 'A':
            ASnumstr = lines.split()[0][2:] #throw away the AS
            AStextlist = lines.split()[1:]
            ASdict[ASnumstr] = " ".join(AStextlist)
    AS.close()
    return ASdict
    #print ASdict['15169']

def read_inputs():
    ASdict = read_asnames()
    far_ts = str(sys.argv[1])
    near_ts = str(sys.argv[2])
    far_loss = str(sys.argv[3])
    near_loss = str(sys.argv[4])
    far_window = str(sys.argv[5])
    mon = far_ts.split('/')[3]
    asnumber = far_ts.split('/')[-1].split('.')[1]
    asname = str(ASdict[asnumber]).replace(",","") #remove commas dt csv format
    far_IP = far_loss.split('/')[-1].strip('.ts')
    month = far_ts.split('/')[-1].split('.')[2]
    return far_loss, near_loss, far_window, far_ts, near_ts, mon, asnumber, asname, far_IP, month

def compute_averages(filename, starts, ends):
    #compute loss averages in and out of window
    avg_cong = -1
    avg_uncong = -1
    avg_diff = 0
    cong = []
    uncong = []

    with open (filename, 'rb') as f:
        reader = csv.reader(f, delimiter=' ')
        for row in reader:
            loss_ts = int(row[0])
            if (is_in_window(loss_ts, starts, ends)):
                cong.append(float(row[1]))
            else:
                uncong.append(float(row[1]))

        if len(cong) == 0:
            avg_cong = -1
            avg_diff = 0
        if len(uncong) == 0:
            avg_uncong = -1
            avg_diff = 0
        if len(cong) > 0 and len(uncong) > 0:
            avg_cong = round(np.mean(cong),3)
            avg_uncong = round(np.mean(uncong),3)
            avg_diff = avg_cong - avg_uncong

    return avg_cong, avg_uncong, avg_diff

def write_averages(monitor, asname, asnumber, far_IP_dots, averages, month):
    avg_far_cong = averages[0]
    avg_far_uncong = averages[1]
    avg_far_diff = averages[2]
    avg_near_cong = averages[3]
    avg_near_uncong = averages[4]
    avg_near_diff = averages[5]

    if avg_far_cong != -1 and avg_near_cong != -1:
        difference = avg_far_diff - avg_near_diff
    else:
        difference = 0

    output_file = '/project/comcast-ping/kabir-plots/loss_data/supplemental_data/' + monitor \
            + '_averages.csv'
    with open (output_file,'a') as f:
        write_string = monitor + "," + month + "," + asname + "," + asnumber + "," + \
                far_IP_dots + "," + str(avg_far_cong) + "," + str(avg_far_uncong) + \
                "," + str(avg_near_cong) + "," + str(avg_near_uncong) +  "," \
                + str(avg_far_diff) + "," + str(avg_near_diff) + "," + str(difference) + '\n'
        f.write(write_string)

def plot_rtt_loss(far_ts_file, window_file, near_ts_file, loss_far_file, loss_near_file):
    if plotting:
        try:
            plotting_command = 'python nohardcoding_heuristics_plotter.py "' + far_ts_file + ' ' \
               + window_file + ' ' + near_ts_file + ' ' + loss_far_file + ' ' + loss_near_file + '"'
            #print plotting_command
            os.system(plotting_command)
        except:
            sys.stderr.write('could not plot files ' + far_ts_file)

def main():

    starts = []
    ends = []
    far_cong = []
    far_uncong = []
    near_cong = []
    near_uncong = []   
    ASdict = read_asnames()
	#compute valid windows
    loss_far_file, loss_near_file, window_file, far_ts_file, near_ts_file, monitor, \
            asnumber, asname, far_IP_dots, month = read_inputs()
    
    starts, ends = read_windows(window_file)

    avg_far_cong, avg_far_uncong, avg_far_diff = compute_averages(loss_far_file, starts, ends)

    avg_near_cong, avg_near_uncong, avg_near_diff = compute_averages(loss_near_file, starts, ends)
   
    averages = [avg_far_cong, avg_far_uncong, avg_far_diff, avg_near_cong, \
            avg_near_uncong, avg_near_diff]

    write_averages(monitor, asname, asnumber, far_IP_dots, averages, month)

    plot_rtt_loss(far_ts_file, window_file, near_ts_file, loss_far_file, loss_near_file)

main()
