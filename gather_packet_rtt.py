from influxdb import InfluxDBClient

from multiprocessing import Pool

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as dates

import datetime
import pytz

#serialization to speed up development of grap generation
import pickle
import os

import json
import operator

import sys

# now = datetime.datetime.now(pytz.UTC)
# start_window = now - (datetime.timedelta(days = 70)) #furthest back we want to see
# end_window = now - datetime.timedelta(days = 61) #closest to now we want to see
# start_ts = start_window.isoformat("T").replace("+00:00", "Z")
# end_ts = end_window.isoformat("T").replace("+00:00", "Z")

#need update to query dynamically on monitor
def get_client_and_query(asn, mon, start_ts, end_ts):
    client = InfluxDBClient('localhost', '<port>', '<username>', '<password>', '<client>', ssl=True)
    query_string = "select mon,target,rtt,link,asn,timestamp,destination from tsplnk where mon='" + mon + "' and asn='" + str(asn) + "\
' and time <= '" + end_ts + "' AND time >= '" + start_ts + "'" 
    
    return client,query_string

def bin_time(time):
    t = float(time)
    t /= 300
    t = int(t)
    t *= 300
    return t

def dict_with_min(d, input):
    key = input[0]
    rtt = input[1]
    if key in d:
        d[key] = min(d[key], rtt)
    else:
        d[key] = rtt

    return d 

def min_dict_to_list(input):
    key = input[0] #(target, link, asn, time, mon, dst)
    rtt = input[1] #rtt
    return (key[0], key[1], rtt, key[2], key[3], key[4], key[5])


def target_link_set(args):
    client = args[0]
    query_string = args[1]

    print query_string

    rs = client.query(query_string)
    points = list(rs.get_points())

    #the set is not needed
    points_list = list()
    for point in points:
        target = point['target']
        link = point['link']
        rtt = point['rtt']
        asn = point['asn']
        mon = point['mon']
        dst = point['destination']
        time = point['time']
        time = bin_time(diff_from_unix_epoch(point['time']))
        time = unix_to_datetime(time)
        time = datetime.datetime.strftime(time, "%Y-%m-%dT%H:%M:%SZ")
        # points_list.append( (target, link, rtt, asn, time, mon) )
        points_list.append( ((target, link, asn, time, mon, dst), rtt) )

    points_dict = reduce(lambda acc, x: dict_with_min(acc, x), points_list, dict())
    points_list = map(min_dict_to_list, points_dict.items())
    points_list.sort(lambda a, b: 1 if a[4] > b[4] else -1)
    return points_list

def convert(date):
    ts = datetime.datetime.strptime(str(date), "%Y-%m-%dT%H:%M:%SZ")
    return dates.date2num(ts)

def diff_from_unix_epoch(ts):
    diff = datetime.datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%SZ") - datetime.datetime(1970,1,1)
    return int(diff.total_seconds())



# as_dict = { 'Netflix':2906, 'Google':15169, 'Akamai':20940, 'Cogent':174, \
#         'Level3':3356, 'Tata':6453, 'Microsoft':8075, 'Limelight':22822, 'Amazon':16509, \
#         'Apple':714, 'Hulu':23286, 'Facebook':32934, 'Twitch':46489, 'Highwinds':33438, \
#         'NTT':2914, 'GTT':3257, 'Zayo':6461, 'Savvis':3561, 'HE':6939, 'XO':2828, 'Telia':1299 }

as_dict = {'Google': 15169}

def make_graph( (near_rtt, near_ts, far_rtt, far_ts, plot_title, file_title, far_packets_lost, far_packets_lost_ls, near_packets_lost, near_packets_lost_ls, far_levelshift_data, is_rtt) ):

    plt.figure()
    curr_fig, ax = plt.subplots()
    curr_fig.subplots_adjust(bottom=0.2)
    ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M'))
    plt.xticks(rotation=90, size='xx-small')
    far_ts = map(convert ,far_ts)
    near_ts = map(convert , near_ts)
    levelshift_to_plot = []

    if is_rtt:
        levelshift_to_plot = far_levelshift_data
        file_title = 'plots/rtt/' + file_title
        plot_title = 'rtt  ' + plot_title
    else:
        levelshift_to_plot = far_packets_lost_ls
        file_title = 'plots/loss/' + file_title   
        plot_title = 'loss ' + plot_title

    for ts,mag in levelshift_to_plot:
            color_val = 'm' if float(mag) < 0 else 'g'
            plt.axvline(x=ts, color=color_val)


    far, = plt.plot_date(far_ts, far_rtt, 'b.', xdate=True, label="far side rtt")
    near, = plt.plot_date(near_ts, near_rtt, 'r.', xdate=True, label="near side rtt")

    plt.xlabel('Time Stamp')
    plt.ylabel('RTT (ms)')
    plt.suptitle(plot_title, fontsize=12)


    twin_ax = None

    if far_packets_lost:
        twin_ax = ax.twinx()
        loss_ts, percent = zip(*far_packets_lost)
        twin_ax.plot(loss_ts, percent, 'ys') #black square
        twin_ax.set_ylabel('Packets Lost (%)')

    if near_packets_lost:
        twin_ax = twin_ax if twin_ax else ax.twinx()
        loss_ts, percent = zip(*near_packets_lost)
        twin_ax.plot(loss_ts, percent, 'gs') #gold square


    plt.savefig(file_title)
    plt.close('all')

    print file_title


def create_graph_args( (link, target_to_measurements, target_to_loss, target_to_loss_ls, rtt_levelshift) ):
    print link
    near, far = link.split(":")
     
    # if near not in target_to_measurements or \
    #     far not in target_to_measurements or \
    #     far not in target_to_loss or \
    #     far not in rtt_levelshift:
    #     return (False, 0)

    near_asn, near_mon, near_data = target_to_measurements[near]
    far_asn, far_mon, far_data = target_to_measurements[far]
    near_rtt, near_ts = zip(*near_data)
    far_rtt, far_ts = zip(*far_data)

    plot_title = near_mon + " --> " + far_asn + " | " + link.replace(":", " --> ")
    file_title = near_mon + "_" + far_asn + "_" + pad_ip_addr(near) + "_" + pad_ip_addr(far) +  ".png"

    far_loss = target_to_loss[str(far)] if str(far) in target_to_loss else []
    far_loss_ls = target_to_loss_ls[str(far)] if str(far) in target_to_loss_ls else []
    near_loss = target_to_loss[str(near)] if str(near) in target_to_loss else []
    near_loss_ls = target_to_loss_ls[str(near)] if str(near) in target_to_loss_ls else []

    # if len(rtt_levelshift[far]) >= 2 or len(far_loss_ls) >= 2:
    return (True, (near_rtt, near_ts, far_rtt, far_ts, plot_title, file_title, far_loss, far_loss_ls, near_loss, near_loss_ls, rtt_levelshift[far]))
    # else:
        # return (False, 0)


def generate_graphs(link_set, target_to_measurements, target_to_loss, target_to_loss_ls, levelshift_data):

    args_list = list()
    pool = Pool(processes = 8)
    args_list = map(lambda link: (link, target_to_measurements, target_to_loss, target_to_loss_ls, levelshift_data), list(link_set))

    graph_args = pool.map(create_graph_args, args_list)
    graph_args = filter(lambda x: x[0], graph_args)

    rtt_ls_graph_args = map(lambda x: x[1] + (True,), graph_args)
    loss_ls_graph_args = map(lambda x: x[1] + (False,), graph_args)


    pool.map(make_graph, rtt_ls_graph_args + loss_ls_graph_args)

def pad_ip_addr(ip_addr):
    return "".join(map(lambda x: x.rjust(3, "0"), ip_addr.split(".")))


def create_time_series_files(target_to_measurements, dirname):

    start_ts = sys.argv[3]
    end_ts = sys.argv[4]

    #key = ip
    #val = ( (asn, dst) , mon, [ (rtt, timestamp) ])
    for key,val in target_to_measurements.items():
        asn_dst, mon, data = val
        asn, dst = asn_dst
        filename = [mon, asn, start_ts, end_ts, key, "ts"] 
        filename = map(str, filename)
        filename = dirname + ".".join(filename)

        with open(filename, 'w+') as fd:
            for rtt,ts in val[2]:
                fd.write(str(diff_from_unix_epoch(ts)) + ' ' + str(int(rtt)) + '\n')

def get_with_defualt(d, key):
    return d[key] if key in d else []


def append_to_dict(d, key, val):
    curr = get_with_defualt(d, key)
    curr += val
    d[key] = curr
    return d

#creates a file for each asn/dst pair and all the rtt data
def create_asn_rtt_book_keeping(target_to_measurements, dirname):

    #target_to_measurements
    # ip --> ( (asn,dst), mon, [(rtt, ts)] )
    try:
        mon = target_to_measurements.items()[0][1][1]
    except IndexError:
        return

    #asn,dst --> [ (rtt, timestamp) ]
    book_keeping = reduce(lambda acc, x: append_to_dict(acc, x[1][0], x[1][2]), target_to_measurements.items(), {})

    for key, val in book_keeping.items():
        asn, dst = key    
        filename = [mon, asn, dst, "ts"]
        filename = ".".join(filename)
        filename = dirname + filename

        with open(filename, 'w+') as fd:
            for rtt,ts in val:
                fd.write(str(diff_from_unix_epoch(ts)) + ' ' + str(int(rtt)) + '\n')

def create_ip_map(links, monitor, dirname):


    filename = dirname + monitor + "." + "ipmap.txt"

    with open (filename, 'w+') as ipmap:
        ipmap = open(filename, "w+")

        for link in links:
            asn, near, far = link
            ipmap.write(asn + " " + near + " " + far + "\n")


def get_or_create_open_file(open_files, asn, monitor, dirname):
    if asn in open_files:
        return open_files[asn]

    fname = dirname + monitor + "." + asn + ".allips.ts"
    open_file = open(fname, "w+")
    open_files[asn] = open_file

    return open_file

def create_all_ip_bk(all_ips, monitor, dirname):

    open_files = dict()
    # all_ip_bk.add( (asn, far, dst) )
    for asn, ts, far, dst, rtt in all_ips:
        open_file = get_or_create_open_file(open_files, asn, monitor, dirname)
        data_point = [diff_from_unix_epoch(ts), far, dst, rtt]
        data_point = [str(x) for x in data_point]
        data_point = " ".join(data_point) + "\n"
        open_file.write(data_point)

def aggregate_measurements(measurement_list):

    target_to_measurements = dict()
    ip_map_bk = set()
    all_ip_bk = list()


    for measurement in measurement_list:
        target = measurement[0]
        link = measurement[1]
        asn = measurement[3]
        rtt = measurement[2]
        timestamp = measurement[4]
        mon = measurement[5]
        dst = measurement[6]
        list_val = (rtt, timestamp)
        near = link.split(":")[0]
        far = link.split(":")[1]
        ip_map_bk.add( (asn, near, far) )
        all_ip_bk.append( (asn, timestamp, far, dst, rtt) )
             
        if target not in target_to_measurements:
            target_to_measurements[target]= ( (asn, dst), mon, [list_val])
        else:
            target_to_measurements[target][2].append(list_val)

    return target_to_measurements, ip_map_bk, all_ip_bk


def unix_to_datetime(ts):
    return datetime.datetime.fromtimestamp(float(ts), pytz.UTC)


#convert line from levelshift output to (matplotlib date, magnitude of levelshift) tuple
def levelshift_to_plot(ls_line):

    ts, rtt, mag = ls_line.split()
    unix_ts = datetime.datetime.fromtimestamp(float(ts), pytz.UTC) #UTC datetime
    plot_date = dates.date2num(unix_ts) #in format matplotlib understands
    
    return (plot_date, mag)


def store_levelshift(filename):

    data = open("timeSeriesAndOutputFiles/" + filename, 'r').readlines()

    return map(levelshift_to_plot, data)
    
def gather_levelshift():

    ip_to_data = dict()

    files = os.listdir("timeSeriesAndOutputFiles")
    files = filter(lambda x: "ts.out" in x, files)
    ip_list = map(lambda x: ".".join(x.split(".")[:4]), files)
    files = map(store_levelshift, files)
    
    for ip, data in zip(ip_list, files):
        ip_to_data[ip] = data

    return ip_to_data


def in_time_range(file_name):
    unix_ts = float(file_name.split('.')[3])
    ts = datetime.datetime.fromtimestamp(unix_ts, pytz.UTC)

    return ts <= end_window and ts >= start_window

def loss_to_dst(loss_obj):
    return loss_obj['responses'][0]['from']

def loss_to_ts(loss_obj):
    return dates.date2num(unix_to_datetime(loss_obj['start']['sec']))

def loss_to_amt(loss_obj):
    return loss_obj['statistics']['loss']

def set_to_dict(d, k , v):
    d[k] = v
    return d

def sanitize_filename(filename):
    a = os.path.basename(filename) #just get file name
    b = os.path.splitext(a)[0] #remove ".out"
    c = os.path.splitext(b)[0] #remove ".ts"
    return c #return ip address

def ls_to_loss(line):
    split = line.split()
    ts, amt, sign = split[0], split[1], split[2]
    ts = unix_to_datetime(ts)
    amt = float(amt) - 1 #added one before levelshift b/c levelshift does not like 0's
    return ts, sign

#convert filename to (target_ip, [lines])
def open_and_read_ls(filename):
    target_ip = sanitize_filename(filename)
    lines = open(filename).readlines()
    lines = map(ls_to_loss, lines)
    return (target_ip, lines)

#return target_ip --> [loss]
def gather_packet_loss_levelshift(dir):
    target_files = os.listdir(dir)
    target_files = filter(lambda x: ".out" in x, target_files)
    target_files = map(lambda x: dir + x, target_files)
    a = map(open_and_read_ls, target_files)
    a = filter(lambda x: len(x[1]) != 0, a)
    return reduce(lambda acc, x: set_to_dict(acc, x[0], x[1]), a, dict())

def ip_to_loss(line):
    split = line.split(' ')
    ts, amt = split[0], split[1]
    ts = unix_to_datetime(ts)
    amt = float(amt) - 1
    return (ts, amt)

def open_and_read(filename):
    target_ip = os.path.basename(filename)
    target_ip = os.path.splitext(target_ip)[0]
    lines = open(filename).readlines()
    lines = map(ip_to_loss, lines)
    return (target_ip, lines)


def gather_packet_loss(dir):
    target_files = os.listdir(dir)
    target_files = filter(lambda x: ".out" not in x, target_files)
    target_files = map(lambda x: dir + x, target_files)
    target_files = map(open_and_read, target_files)
    return reduce(lambda acc, x: set_to_dict(acc, x[0], x[1]), target_files, dict())

def arg_time_to_query_time(ts):
    output_ts = datetime.datetime.strptime(ts, "%m_%d_%Y")
    output_ts = str(output_ts)
    output_ts = output_ts.replace(" ", "T") + ".000000Z"
    return output_ts

def arg_time_to_query_time_end(ts):
    output_ts = datetime.datetime.strptime(ts, "%m_%d_%Y")
    output_ts = str(output_ts)
    output_ts = output_ts.replace(" ", "T") + ".000000Z"
    output_ts = output_ts.replace("00:00:00", "23:59:59")
    return output_ts

def parse_as_list(fname):
    f = open(fname).read()
    f = f.split("\n")
    f = f[:-1] #last character is newline, creates extra entry we need to drop
    return f

def process_args():
    if len(sys.argv) != 5:
        print("incorrect number of arguments")
        print("Usage: main_script.py [monitor] [path_to_as_list] [start_ts] [end_ts]")
        exit()

    mon = sys.argv[1]
    as_list = parse_as_list(sys.argv[2])
    start_ts = arg_time_to_query_time(sys.argv[3])
    end_ts = arg_time_to_query_time_end(sys.argv[4])
    return mon, as_list, start_ts, end_ts

def main(): 

    # os.system('rm -rf timeSeriesAndOutputFiles')
    # os.system('mkdir timeSeriesAndOutputFiles')
    # os.system('rm -rf plots')
    # os.system('mkdir plots')
    # os.system('mkdir plots/rtt')
    # os.system('mkdir plots/loss')

    measurement_list = list()
    target_to_measurements = dict() #(target_ip) -->  (asn, mon, [ (rtt, timestamp) ])
    link_set = set()


    mon, as_list, start_ts, end_ts = process_args()
    rtt_ls_dirname = "rtt_and_loss_data/rtt/levelshift/" + mon + "/" + sys.argv[3] + "_" + sys.argv[4] + "/"
    rtt_bk_dirname = "rtt_and_loss_data/rtt/book_keeping/" + mon + "/" + sys.argv[3] + "_" + sys.argv[4] + "/"
    
    os.system('mkdir -p ' + rtt_ls_dirname)

    os.system('mkdir -p ' + rtt_bk_dirname)
    
    pool = Pool(processes = 8)

    #create list of (client, query) tuples
    client_query_list = [get_client_and_query(asn, mon, start_ts, end_ts) for asn in as_list]

    print "Making Query"

    # list of sets containing (target, link, rtt, asn, timestamp)
    target_link_list = pool.map(target_link_set, client_query_list)

    print "Done with Query"

    #reduce list of lists into list
    measurement_list = reduce(lambda x,y: x+y, target_link_list)

    print "Reduced Data"

    #create dict from measurements
    target_to_measurements, ip_map_bk, all_ip_bk = aggregate_measurements(measurement_list) 

    print "Created target dict"

    #prepare for levelshift by writing files
    create_time_series_files(target_to_measurements, rtt_ls_dirname)
    create_asn_rtt_book_keeping(target_to_measurements, rtt_bk_dirname)
    create_ip_map(list(ip_map_bk), mon, rtt_bk_dirname)
    create_all_ip_bk(all_ip_bk, mon, rtt_bk_dirname)


    print "Done with Writing Files"

    os.system("python levelShiftOnInflux.py " + rtt_ls_dirname)

    # print "Reading in levelshift data"
    # levelshift_dict = gather_levelshift()
    
    # print "Start making graphs"

    #generate_graphs(link_set, target_to_measurements, target_to_loss, target_to_loss_ls, levelshift_dict)

    print "Done."

main()
