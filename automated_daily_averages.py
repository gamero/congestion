#usage: python automated_loss_averages.py far_ts near_ts far_loss near_loss far_window
#input: rtt, loss, and congestion window files
#if <plotting_bool> is true, a plot is attempted
#output: average loss in congested periods and outside of it
#output is "nan" or -1 if there is no packet loss data inside the congested window
#output format is 
#monitor,asname,far_IP,avg_far_congested,avg_far_uncongested,avg_near_congested,far_diff,near_diff,diff_diff
import csv
import numpy as np
import time
import sys
import os
from daily_support_functions import *
from scipy import stats

plotting = True
#plotting = False

def read_windows(filename):
    h = open(filename, 'rb+')
    starts = []
    ends = []
    rtt_metrics = []
    reader = csv.reader(h, delimiter=' ')
    for row in reader:
        metric = ''
        starts.append(int(row[0]))
        ends.append(int(row[1]))
        metric = str(row[2]) + ';' + str(row[3]) #also grab rtt metrics
        rtt_metrics.append(metric)
    return starts, ends, rtt_metrics

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
    near_IP = near_loss.split('/')[-1].strip('.ts')
    month = far_ts.split('/')[-1].split('.')[2]
    month_end = far_ts.split('/')[-1].split('.')[3]
    return far_loss, near_loss, far_window, far_ts, near_ts, mon, asnumber, \
            asname, far_IP, month, month_end, near_IP

def compute_averages(data_list, starts, ends, day_begin, rtt_metrics, cong_lost, cong_sent, uncong_lost, uncong_sent):
    #compute loss averages in and out of window
    avg_cong = -1
    avg_uncong = -1
    avg_diff = 0
    cong = []
    uncong = []
    split = 0
    total_seconds = 0
    window_string = ''

    day_end = day_begin + 86399

    if len(starts) > 0: # if there is at least one window to analyze
        
        for i in range(len(starts)):
        
            if is_in_day( day_begin, starts[i], ends[i] ): 
                split_tmp = 0
                interval, split_tmp = window_day(day_begin, starts[i], ends[i], split )#build interval based on day boundaries
                split = split + split_tmp
                window_start = [ interval[0] ]
                window_end = [ interval[1] ]

                total_seconds = total_seconds + window_end[0] - window_start[0]

                window_string = window_string + ';' + str(window_start[0]) + ';' + str(window_end[0]) + ';' + \
                        str(rtt_metrics[i])
                for row in range(len(data_list)): #go through data for windows that are IN day

                    loss_ts = data_list[row][0] #timestamp

                    if (is_in_window(loss_ts, window_start, window_end)):
                        cong.append( data_list[row][1] )
                        cong_sent = cong_sent + int(data_list[row][3])
                        cong_lost = cong_lost + int(data_list[row][2])
                        
                    else:
                        uncong.append( data_list[row][1] )
                        uncong_sent = uncong_sent + int(data_list[row][3])
                        uncong_lost = uncong_lost + int(data_list[row][2])
    
    elif len(starts==0):
        for row in range(len(data_list)):
            uncong.append( data_list[row][1] )

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
    elif len(uncong) > 0:
        avg_cong = round(np.mean(cong),3)
        avg_diff = 0
    window_string = window_string + ';' + str(split)
    return avg_cong, avg_uncong, avg_diff, total_seconds, window_string, uncong_lost, uncong_sent, cong_lost, cong_sent

def write_averages(monitor, asname, asnumber, far_IP_dots, \
        averages, day_string, metrics, window_string, near_IP_dots):
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
    human_time = str(time.strftime('%Y-%m-%dT00:00:00Z', time.gmtime(day_string)))
    output_file = '/project/comcast-ping/kabir-plots/loss_data/supplemental_data/' + monitor \
            + '_daily.csv'
    with open (output_file,'a') as f:
        write_string =  str(monitor) + "," + human_time + "," +str(day_string) + "," + str(asname) + "," + str(asnumber) \
                + "," + far_IP_dots + "," + near_IP_dots + "," + str(avg_far_cong) + "," + str(avg_far_uncong) + \
                "," + str(avg_near_cong) + "," + str(avg_near_uncong) +  "," \
                + str(avg_far_diff) + "," + str(avg_near_diff) + "," + str(difference) + \
                 ',' + str(metrics) + ',' + str(window_string) + '\n'
        f.write(write_string)
    

def plot_rtt_loss(far_ts_file, window_file, near_ts_file, loss_far_file, loss_near_file):
    if plotting:
        try:
            plotting_command = 'python nohardcoding_heuristics_plotter.py "' + far_ts_file + ' ' \
               + window_file + ' ' + near_ts_file + ' ' + loss_far_file + ' ' + loss_near_file + '"'
            print plotting_command
            os.system(plotting_command)
        except:
            sys.stderr.write('could not plot files ' + far_ts_file)

def read_loss_data(filename):
    loss_list = []
    #read loss data into memory
    with open (filename, 'rb') as f:
        reader = csv.reader(f, delimiter=' ')
        for row in reader:
            loss_list.append( [ int( row[0] ) , float( row[1] ) , int( row[2] ), int( row[3] ) ] )
    return loss_list

def read_rtt_timestamps(filename):
    timestamp_list = []
    #read loss data into memory
    try:
        command = 'gunzip -f ' + filename + '.gz'
        os.system(command)
    except:
        sys.stderr.write('could not gunzip ' + filename)

    with open (filename, 'rb') as f:
        reader = csv.reader(f, delimiter=' ')
        for row in reader:            
            timestamp_list.append( [int( row[0] ), int( row[1] ) ] )
            
    try:
        command = 'gzip -f ' + filename
        os.system(command)
    except:
        sys.stderr.write('could not gzip ' + filename)
    return timestamp_list


def analyze_days( day_starts, loss_far_list, starts, ends, loss_near_list, far_timestamps, near_timestamps,\
        monitor, asname, asnumber, far_IP_dots, near_IP_dots, rtt_metrics):

    #loss counts for statistical testing
    far_uncong_lost = far_uncong_sent = far_cong_lost = far_cong_sent = 0
    near_uncong_lost = near_uncong_sent = near_cong_lost = near_cong_sent = 0

    for day in day_starts:
        
        avg_far_cong, avg_far_uncong, avg_far_diff, total_seconds, window_string, \
                far_uncong_lost, far_uncong_sent, far_cong_lost, far_cong_sent = \
                compute_averages(loss_far_list, starts, ends, day, rtt_metrics, far_cong_lost, far_cong_sent, far_uncong_lost, far_uncong_sent)

        avg_near_cong, avg_near_uncong, avg_near_diff, nothing, window_string, \
                near_uncong_lost, near_uncong_sent, near_cong_lost, near_cong_sent = \
                compute_averages(loss_near_list, starts, ends, day, rtt_metrics, near_cong_lost, near_cong_sent, near_uncong_lost, near_uncong_sent)

        averages = [avg_far_cong, avg_far_uncong, avg_far_diff, avg_near_cong, \
                avg_near_uncong, avg_near_diff]

        day_window = [day, (day+86400)]

        #compute uptime for loss and rtt, both far and near ends
        loss_far_uptime = filter_uptime(day_window, loss_far_list)
        loss_near_uptime = filter_uptime(day_window, loss_near_list)

        rtt_far_uptime = filter_uptime(day_window, far_timestamps)
        rtt_near_uptime = filter_uptime(day_window, loss_far_list)
        
        congestion_minutes = round( (total_seconds / 60),1)
        
        if int(rtt_far_uptime) > 0:
            congestion_percentage = round( ( total_seconds * float(rtt_far_uptime) / (86400) ),1)
        else:
            congestion_percentage = -1

        metrics =  str(congestion_minutes) + ',' + str(congestion_percentage) + ',' + str(loss_far_uptime) + \
                ',' + str(loss_near_uptime) + ',' + str(rtt_far_uptime) + ',' + str(rtt_near_uptime)
                
        write_averages(monitor, asname, asnumber, far_IP_dots, averages, day, metrics, window_string, near_IP_dots)
    
    summary_file = str(sys.argv[1]).strip('.ts') + '.sum.txt'
    with open (summary_file, 'w') as f:
        sys.stderr.write('writing loss summary file ' + summary_file + '\n')
        f.write(str(monitor) + ',' + str(asnumber) + ',' + str(far_IP_dots) + ',' + str(near_IP_dots) + '\n')
        
        far_uncong = [1] * (far_uncong_sent - far_uncong_lost) + [0] * far_uncong_lost
        far_cong = [1] * (far_cong_sent - far_cong_lost) + [0] * far_cong_lost    
        fisher_coeff, pvalue = stats.ttest_ind(far_uncong, far_cong)
        f.write('far' + ',' + str(far_uncong_lost) + ',' + str(far_uncong_sent-far_uncong_lost) + ',' \
                + str(far_cong_lost) + ',' + str(far_cong_sent-far_cong_lost) + ',' + str(round(fisher_coeff,3)) +
                ',' + str(round(pvalue,3)) +'\n' )

        near_uncong = [1] * (near_uncong_sent - near_uncong_lost) + [0] * near_uncong_lost
        near_cong = [1] * (near_cong_sent - near_cong_lost) + [0] * near_cong_lost
        fisher_coeff, pvalue = stats.ttest_ind(near_uncong, near_cong)
        f.write('near' + ',' + str(near_uncong_lost) + ',' + str(near_uncong_sent-near_uncong_lost) + \
                ',' + str(near_cong_lost) + ',' + str(near_cong_sent) + ',' + str(round(fisher_coeff,3)) + \
                ',' + str(round(pvalue,3)) )
#    fisher_coeff, pvalue = stats.fisher_exact([near_uncong, near_cong])
#    print (" p = ") + str(pvalue)

def main():

    starts = []
    ends = []
    rtt_metrics = []
    ASdict = read_asnames()
    
    #read inputs and transform into usable strings

    loss_far_file, loss_near_file, window_file, far_ts_file, near_ts_file, monitor, \
            asnumber, asname, far_IP_dots, month, month_end, near_IP_dots = read_inputs()
    
    #create list with beginnings and ends of days for the period in far-ts file

    day_starts = daily_seconds(month, month_end)

    #read window starts
    starts, ends, rtt_metrics = read_windows(window_file)

    #read loss data from file into memory
    loss_far_list = read_loss_data( loss_far_file )
    loss_near_list = read_loss_data( loss_near_file )

    #grab rtt timestamps to compute uptime later
    rtt_far_timestamps = read_rtt_timestamps(far_ts_file)
    rtt_near_timestamps = read_rtt_timestamps(near_ts_file)
    
    analyze_days( day_starts, loss_far_list, starts, ends, loss_near_list, rtt_far_timestamps, rtt_near_timestamps,\
                    monitor, asname, asnumber, far_IP_dots, near_IP_dots, rtt_metrics)

    #avg congestion 
    plot_rtt_loss(far_ts_file, window_file, near_ts_file, loss_far_file, loss_near_file)

main()
