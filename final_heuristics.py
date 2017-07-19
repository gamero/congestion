#usage: python cong_analysis.py <far_end_ts_filename> <near_ts_filename>
#.out files
#Filters far-end levelshifts with overlapping windows
#outputs congestion periods, total congestion time (minutes), number of levelshifts detected
#and uptime of both near-end and far-end based on raw ts data
import csv
import numpy as np
import sys
import os


path = '/project/comcast-ping/plots-agamerog/' + str(sys.argv[1]).split('.')[0] + '/' + str(sys.argv[1]).split('.')[2] + '/'
monitor = str(sys.argv[1]).split('.')[0]
shifts_file = '/project/comcast-ping/plots-agamerog/heuristics_' + monitor + '.txt'
far_filename = path + str(sys.argv[1]) + '.ls.txt.out'
near_filename = path + str(sys.argv[2]) + '.ls.txt.out'
raw_near = path + str(sys.argv[2])
raw_far = path + str(sys.argv[1])
month = int(str(sys.argv[1]).split('.')[2])
half_hour = 1800
quarter_hour = 900
full_hour = 3600
two_hours = 7200
one_day = 86400
filtering_magnitude = 10

#DICTIONARY WITH BEGINNINGS AND ENDS OF MONTH IN UNIX EPOCH
beginnings = { 201604:'1459468800',\
        201605:'1462060800',\
        201606:'1464739200',\
        201607:'1467331200',\
        201608:'1470009600',\
        201609:'1472688000',\
        201610:'1475280000',\
        201611:'1477958400',\
        201612:'1480550400',\
        201701:'1483228800',\
        201702:'1485907200',\
        201703:'1488326400'
        }

ends = { 201604:'1462060799',\
        201605:'1464739199',\
        201606:'1467331199',\
        201607:'1470009599',\
        201608:'1472687999',\
        201609:'1475279999',\
        201610:'1477958399',\
        201611:'1480550399',\
        201612:'1483228799',\
        201701:'1485907199',\
        201702:'1488326399',\
        201703:'1491004799'
        }

def uptime_windows(windows, far_data, near_data, err_string):
    output_windows = []
    
    for i in range(len(windows)):
        far_uptime = filter_uptime(windows[i], far_data)
        near_uptime = filter_uptime(windows[i], near_data)
#        print str(far_uptime) + "\n"
#        print str(near_uptime) + "\n"
        if far_uptime >= 90.0 and near_uptime >= 90.0:
            output_windows.append(windows[i])
        else:
            err_ts = str(windows[i][0]) + " " + err_string
            sys.stderr.write('%s fails uptime (90%) test\n' % err_ts)
    return output_windows

def filter_uptime(window, data):
    #compute uptime of both near- and far-side raw rtt data within window limits
    comparer_buffer = 0
    uptime = 0
    end_time = int(window[1])

    for line in range(len(data)):
        comparer = int(data[line].split(' ')[0])
        time_loop = int(window[0])
        #ignore repeated measurements for the same 5-minute cycle
        if comparer_buffer == comparer:
                continue

        comparer_buffer = comparer
        while time_loop <= end_time:
            lower = time_loop
            upper = time_loop + 300
            time_loop = time_loop + 300
            if (comparer <= upper) and (comparer >= lower ):
                uptime = uptime + 1
                break
            else:
                continue

    hop_loop = int(window[0])
    hop_counter = 0
    while hop_loop <= end_time:
            hop_counter = hop_counter + 1
            hop_loop = hop_loop + 300  
#    print str(uptime) + "\n"
    return (round(float(uptime)*100/float(hop_counter),1))   

def write_windows(filename, windows, output_separation, data, filter_bool, second_data):
    #writes windows to file in format +ts -ts, one per line
    #optionally also writes separation to file 
    windows_file = str(filename) + '.win.txt'

    if len(second_data) > 0: #output magnitude of far-end minus near-end shift
        differential = True
    else:
        differential = False

    try:
        f = open(windows_file, 'w+')
    except:
        sys.stderr.write("could not open file for writing" + windows_file)
        return

    for i in range(len(windows)):
        mag, post, pre = shift_magnitude(data, windows[i][0], half_hour)
        if mag >= 3: 
            if differential:
                near_mag, nothing, nothing = shift_magnitude(second_data, windows[i][0], half_hour)
                if near_mag > 0:
                    mag = mag - near_mag
            write_string = str(windows[i][0]) + " " + str(windows[i][1]) + " " + str(mag) \
            + " " + str(pre) + " " + str(post)
            f.write(write_string)
            f.write("\n")
        else:
            sys.stderr.write("window fails sanity check for magnitude " + str(windows[i][0]) + "\n")
    if output_separation:
        separation_file = str(filename) + '.sep.txt'
        
        try:
            g = open(separation_file, 'w+')
            for i in range(len(windows)):
                if i < (len(windows)-1) and len(windows) > 1:
                        separation = windows[i+1][0] - windows[i][1]
                        g.write(str(separation))
                        g.write("\n")        
        except:
            sys.stderr.write("could not open file for writing" + separation_file)
            return

def shift_magnitude(data, timestamp, period):
        #returns
        #the post-levelshift (period) rtt average and pre-levelshift (period) average
        #input is data in tuples: [unix_timestamp, rtt] 
        upper = timestamp + period
        lower = timestamp - period
        post_shift = []
        pre_shift = []
        comparer_buffer = 0
        magnitude = 0
        for line in range(len(data)):

        # ensure loop only looks at window of interest around timestamp

                comparer = int(data[line].split(' ')[0])

                if comparer > upper:
                        break

                if comparer < lower:
                        continue

        # create arrays with pre- and post-shift rtt values
                if comparer >= lower and comparer < timestamp:
                        pre_shift.append(int(data[line].split(' ')[1]))

                if comparer <= upper and comparer > timestamp:
                        post_shift.append(int(data[line].split(' ')[1]))
        if len(post_shift) > 0 and len (pre_shift) > 0:
                avg_post_shift = np.mean(post_shift)
                avg_post_shift = round(avg_post_shift, 2)
                avg_pre_shift =  np.mean(pre_shift) 
                avg_pre_shift = round(avg_pre_shift, 2)
                magnitude = abs( avg_post_shift - avg_pre_shift ) # due to data stored in db *10
                magnitude = round(magnitude, 2)
        else:
                magnitude = 0
                avg_pre_shift = 0
                avg_post_shift = 0
        return magnitude, avg_post_shift, avg_pre_shift

def levelshift_windows(triplets, data):
	
    #windows will have the timestamps of rising and falling edges of valid elevated rtt periods
    windows = []
    merging_positives = []
    merging_negatives = []
    current_positives = [] #temp to filter consecutive positives
    current_negatives = []
    lone_shifts = []
    for i in range( len(triplets) ): #INDEX NEEDS UPDATING
    #Look for positive shifts, see if next shift is negative, otherwise filter
        
                #two windows could be merged. 
        current_negatives = []	
        current_positives = []

        if len(merging_positives) > 0 and len(merging_positives) > 0:
                #merging adjacent windows
                 current_negatives = merging_negatives
                 current_positives = merging_positives

        merging_positives = []
        merging_negatives = []

        if triplets[i][1] > 0:
            #determine if following shift occurs within 24h. Might want to insert window verifier for lone shifts
             
            #last element is a positive. add element and any preceding positives to lone shift array
            if i == (len(triplets)-1):
                lone_shifts.append(triplets[i])
                if len(current_positives) > 0:
                        for n in range(len(current_positives)):
                                lone_shifts.append(current_positives[n])
                break
            time_difference = triplets[i+1][0] - triplets[i][0]
            if time_difference > one_day and len(current_positives) == 0:
                lone_shifts.append(triplets[i])
                continue
            current_positives.append(triplets[i])

            #a nagative follows a positive. Determine which negative to use if more than one
            
            if triplets[i+1][1] < 0:
                    output_positive_timestamp = current_positives[0][0]
                    output_negative_timestamp = triplets[i+1][0]
                    j = i + 1
                                
                    #Store negative shifts in either current array or lone shift (beyond 24 hours)

                    while triplets[j][1] < 0:

                        time_difference = triplets[j][0] - output_positive_timestamp
                        #Next shift happens more than a day later, ignore

                        if time_difference > one_day:
                            break
                            
                        current_negatives.append(triplets[j])
                        if j == (len(triplets) - 1):
                            break
                        j = j + 1
                                    
                            #See if following shift happens within two hours. 
                            
                    if j < len(triplets) and j > 0: # shift isn't last shift, more than one shift to compare
                            time_difference = triplets[j][0] - triplets[j-1][0]

                            #Next shift is within two hours and positive: merge windows

                            if time_difference < two_hours and triplets[j][1] > 0:
                                    merging_positives = current_positives
                                    merging_negatives = current_negatives
                                    continue        
                            
                    #detect if there are positive shifts and no negative shifts or viceversa

                    if len(current_negatives) == 0 and len(current_positives) > 0:
                        for m in range(len(current_positives)):
                            lone_shifts.append(current_positives[m])
                        continue

                    if len(current_negatives) > 0 and len(current_positives) == 0:
                        continue
                    #exactly one negative follows exactly one positive. output window without verification.
                    if len(current_negatives) == 1 and len(current_positives) == 1:
                        
                        output_negative_timestamp = current_negatives[0][0]
                        testing_window = [output_positive_timestamp, output_negative_timestamp]
                        good_window = False
                        good_window, good_timestamp = threshold_crossing(testing_window, data)
                        #if rtt crosses down threshold during window, use crossing point as end of window
                        if not good_window:
                            output_negative_timestamp = good_timestamp

                        duration = output_negative_timestamp - output_positive_timestamp

                        if duration < one_day and duration > 0: 
                            #make sure window is less than one day wide
                            windows.append([output_positive_timestamp,output_negative_timestamp])
                            continue

                    #more than one negative or positive
                    match_found = False
                    match_output = False
                    
                    #For each positive, starting with the earliest timestamp, see if there is an average matching in
                    #one of the negatives starting with the latest timestamp
                    
                    for l in range(len(current_positives)):

                            if match_found:
                                    break
                            output_positive_timestamp = current_positives[l][0]
                            match_found = False
                            good_window = False
                            #Loop to go through negatives in reverse order
                            #see if window passes threshold test
                            for k in reversed(range(len(current_negatives))):            
                                    testing_window = \
                                            [output_positive_timestamp, output_negative_timestamp]
                                    good_window, good_timestamp = threshold_crossing(testing_window, data)
                                    
                                    if good_window:
                                            output_negative_timestamp = current_negatives[k][0]
                                            match_found = True
                                            duration = output_negative_timestamp - output_positive_timestamp
                                            if duration < one_day and duration > 0:
                                                    windows.append([output_positive_timestamp,output_negative_timestamp])
                                                    match_output = True
                                                    
                                                    break

                    #bipartite matching failed. Find max rsi for both negative and positive shifts, output pair                    
                    if len(current_positives) > 0 and len(current_negatives) > 0 and not match_output:
                        
                        #special cases with one element are outputted automatically
                        #otherwise, find negative timestamp where rtt crosses down threshold

                        if len(current_positives) == 1:
                            output_positive_timestamp = current_positives[0][0]
                            end_timestamp = output_positive_timestamp + one_day
                            testing_window = [output_positive_timestamp, end_timestamp]
                            nothing, output_negative_timestamp = threshold_crossing(testing_window, data)
                        else:
                            current_ending = 9999999999 #initialize as "infinity"

                            for l in range(len(current_positives)): #no match found. use threshold crossing

                                if current_positives[l][0] > current_ending:
                                    continue #ignore values that would overlap with a previously outputted window

                                output_positive_timestamp = current_positives[l][0]
                                end_timestamp = output_positive_timestamp + one_day
                                testing_window = [output_positive_timestamp, end_timestamp]
                                nothing, output_negative_timestamp = threshold_crossing(testing_window, data)
                                current_ending = output_negative_timestamp
                           
                                duration = output_negative_timestamp - output_positive_timestamp
                                if duration < one_day and duration > 0: #make sure window is less than one day wide
                                    windows.append([output_positive_timestamp,output_negative_timestamp])
                                    continue
            
        else:
            continue
        #print "number of lone shifts = " + str(len(lone_shifts)) 
        if len(lone_shifts) > 0:
                current_positives = lone_shifts
                
                current_ending = 9999999999 #initialize as "infinity"

                for l in range(len(current_positives)): #no match found. use threshold crossing

                    if current_positives[l][0] > current_ending:
                        continue #ignore values that would overlap with a previously outputted window

                    output_positive_timestamp = current_positives[l][0]
                    end_timestamp = output_positive_timestamp + one_day
                    testing_window = [output_positive_timestamp, end_timestamp]
                    nothing, output_negative_timestamp = threshold_crossing(testing_window, data)
                    current_ending = output_negative_timestamp

                    duration = output_negative_timestamp - output_positive_timestamp
                    if duration < one_day and duration > 0: #make sure window is less than one day wide
                        windows.append([output_positive_timestamp,output_negative_timestamp])
                        continue

    return windows

def window_discarder(far, near, far_data, near_data, discarding_magnitude, self_discarding):
        final_windows = []

        #determine if there is any near-end window overlap in each far end window. If overlap > 50%, ignore window.
        for i in range(len(far)):             
                
                lower = far[i][0]
                upper = far[i][1]
                far_magnitude, nothing, nothing = shift_magnitude(far_data, lower, half_hour)
                overlap = 0
                invasion = []
                invasion_beginnings = []
                contained = False

                for j in range(len(near)):
                        near_lower = near[j][0]
                        near_upper = near[j][1]

                        #next two conditions: no overlap
                        if (near_lower >= upper):
                                continue
                        if (near_upper <= lower):
                                continue
                        #far window completely inside near window (discard)
                        if (lower >= near_lower and upper <= near_upper):
                                contained = True
                                break
                        #near window completely inside far window

                        if (near_lower >= lower and near_lower <= upper and near_upper >= lower and near_upper <= upper):
                                invasion.append(near_upper - near_lower)
                                invasion_beginnings.append(near_lower)
                        #near window partially inside far window
                        elif near_lower >= lower and near_lower <= upper:
                                invasion.append(upper - near_lower)
                                invasion_beginnings.append(near_lower)
                        else:
                                invasion.append(near_upper - lower)
                                invasion_beginnings.append(lower)

                if contained == True:
                        #far-end window completely contained in near-window. determine magnitude differential
                        #Also used for the self-discarding phase
                        near_magnitude, nothing, nothing = shift_magnitude(near_data, near_lower, half_hour)
                        
                        difference_magnitude = far_magnitude - near_magnitude
                        if difference_magnitude <= discarding_magnitude:
                                err_ts = str(lower) + " " + str(difference_magnitude)
                                
                                sys.stderr.write('%s contained far fails magnitude test\n' % err_ts)
                                continue
                        else:
                                #some far-side congetion still not explained by near-side? 
                                #more stringent test for self-discarding windows

                                if(self_discarding):
                                    nothing, post_far, nothing = shift_magnitude(far_data, lower, half_hour)
                                    nothing, post_near, nothing = shift_magnitude(near_data, near_lower, half_hour)
            
                                    difference_level = post_far - post_near
                                    if difference_level >= 15:
                                        #print "passed level test with level = " + str(difference_level)
                                        final_windows.append(far[i])
                                        continue
                                    else:
                                         err_ts = str(lower)
                                         sys.stderr.write('%s contained far fails level test\n' % err_ts)
                                        
                                         continue
                                else:
                                    final_windows.append(far[i])
                                    continue
                if len(invasion) == 0:
                        final_windows.append(far[i])
                        continue

                far_duration = upper - lower
                keeping_boolean = True
                #discard windows that overlap 50% of the time and near-side explains congestion
                for k in range(len(invasion)):
                        current_overlap = (int(invasion[k] *100 / far_duration))
                        
                        if current_overlap >= 50:
                                near_magnitude, nothing, nothing = shift_magnitude(near_data, invasion_beginnings[k], half_hour)
                                difference_magnitude = far_magnitude - near_magnitude
                                
                                if difference_magnitude <= 5:
                                        keeping_boolean = False
                                        err_ts = str(far[i][0])
                                        sys.stderr.write('%s far fails overlap/magnitude test (exp. by near side)\n' % err_ts)
                                        break
                                        
                if keeping_boolean:
                        final_windows.append(far[i])
        return final_windows

def metrics(windowsf):
	duration = 0
	separation = 0
	for i in range(len(windowsf)):
		duration = duration + windowsf[i][1] - windowsf[i][0]
		if i < (len(windowsf)-1) and len(windowsf) > 1:
			separation = separation + windowsf[i+1][0] - windowsf[i][1]

	return (duration/60), (separation/60)

def threshold_crossing(window, data):
    #Determines internal consistency of a window: does the 15-min average
    # (min) rtt cross below (pre-shift-avg + 5 ms)?
    # inputs: window timestamps, rtt (min) data
    # outputs: binary with whether or not threshold crossed
    # if threshold crossed, timestamp of the end of last 15-min period 
    # before threshold is crossed    

    no_cross_bool = True
    cross_timestamp = 0    

    end_time = int(window[1])
    start_time = int(window[0])
    nothing, nothing, pre_shift = shift_magnitude(data, start_time, half_hour)
    threshold = pre_shift + 3
    threshold = round(threshold,1)
    time_loop = start_time + 900
#    print "threshold = " + str(threshold)
    
    while time_loop <= end_time:
        nothing, current_average, nothing = shift_magnitude(data, time_loop, quarter_hour)
        current_average = round(current_average,1)
        if current_average < threshold:
            no_cross_bool = False
            cross_timestamp = time_loop       
            break
        time_loop = time_loop + 900

# Save error message for reference (where threshold test broke)a
    if time_loop == (start_time + 900):
        err_ts = str(start_time)
        cross_timestamp = start_time
        #print "current_average = " + str(current_average)
        sys.stderr.write('%s failed threshold test (crosses back th in second 15 min block)\n' % err_ts)
#    elif no_cross_bool == False:
#        err_ts = str(start_time) + " threshold crossing at " + str(cross_timestamp) 
#        sys.stderr.write('%s \n' % err_ts)    
#    if no_cross_bool == True:
#        print "threshold never crossed"
    return no_cross_bool, cross_timestamp

def filter_minimums(data):

    #compute uptime of both near- and far-side raw rtt data within window limits
    comparer_buffer = 0
    uptime = 0

    rtts = []
    timestamps = [] #store current rtt values
    start_time = int(int(data[0].split(' ')[1]) / 300) * 300
    end_time = int(int(data[-1].split(' ')[1]) / 300) * 300

    # break ARTIFICIAL END TIME HERE
#    end_time = 1459728000
    number_bins = int((end_time - start_time) / 300)
    mins = [0] * number_bins
    output_array = []
#    print "number of bins" + str(number_bins)
    for times in range(len(mins)):
        #THIS LINE WILL ALSO HAVE TO GOA
        index = start_time + times * 300
#        if index > 1459728000:
#            break
        timestamps.append((start_time + times * 300))

    for line in range(len(data)):
        comparer = int(data[line].split(' ')[1])
#        if comparer > 1459728000:
#            break

        time_loop = int(int(data[line].split(' ')[1]) / 300) * 300 #get the quotient of the start time (5 min bin)
        #ignore repeated measurements for the same 5-minute cycle
        if comparer_buffer == comparer:
                continue
        #print "CP1"
        comparer_buffer = comparer
        j = 0
        while j < number_bins:

            lower = start_time + j*300
            upper = lower + 300
            if (comparer <= upper) and (comparer >= lower ):
            #determine 5 minute minimum and save to array element with associated timestamp
                if mins[j] == 0:
                    mins[j] = int(data[line].split(' ')[4])/10
                    
                else:
                    temp = int(data[line].split(' ')[4])/10
                    mins[j] = min(mins[j], temp)
                    
            j = j + 1
    #transformation to keep data structure readable by further funcitons 
    for k in range(len(mins)):
        write_buffer = str(timestamps[k]) + ' ' + str(mins[k])
        output_array.append(write_buffer)
    return (output_array)

def reading_files():
        #reads data from files
        processed = 0 #boolean to determine whether or not a file has been processed
        number_files = 2
        filter_window = 900 #number of seconds on either side
        filtering_threshold = 4 # how many levelshifts make a link "congested"
        #Read all filenames provided
        far = []
        near = []
        far_positives = []
        far_negatives = []
        near_positives = []
        near_negatives = []
        positives_filtered = []
        negatives_filtered = []
        congestion_minutes = 0
        congestion_periods = 0
        far_uptime = 0
        near_uptime = 0
        comparer_buffer = 0
        for j in range(number_files):
        #Get information about file from inputs
            if j == 0:
                    filename = far_filename
                    raw = raw_far
            else:
                    filename = near_filename
                    raw = raw_near

    #Load raw data into memory

            try:
                    #f = open(raw, 'r+')#import file
                    gunzipper = raw + '.gz'
                    try:
                            command = "gunzip " + gunzipper
                            os.system(command)
                    except:
                            print "file not gzipped"
                    f = open(raw)
                    data = f.readlines()
                    command = "gzip " + raw
                    try:
                        os.system(command)
                    except:
                        sys.stderr.write('could not gzip file %s\n' % raw)
                    if j == 0:
                        far_data = data
                    else:
                        near_data = data
            #Check for OS and IO errors
            except OSError as o:
                    sys.stderr.write('raw file error: %s\n' % o)
#                    f.close
                    return
            except IOError as i:
                    sys.stderr.write('File open failed: %s\n' % i)
#                    f.close
                    return
            except FileEmptyError as p:
                    sys.stderr.write('ts file error: %s\n' %p)
#                    f.close
                    return
            
            #Read levelshift data into 3-tuple (timestamp, rsi, magnitude)

            try:
                    f = open(filename, 'r+')#import file
            #Check for OS and IO errors
            except OSError as o:
                    sys.stderr.write('levelshift file error: %s\n' % o)
#                    f.close
                    return
            except IOError as i:
                    sys.stderr.write('File open failed: %s\n' % i)
#                    f.close
                    return
            except FileEmptyError as p:
                    sys.stderr.write('levelshift file error: %s\n' %p)
#                    f.close
                    return
            else:
                    sys.stderr.write('reading levelshift file %s\n' % filename)
                    reader = csv.reader(f, delimiter='\t')
                    #if j == 0:
                            #processed = 1
                    #Read values from file
                    for row in reader:
                    #storing timestamps and magnitude of shift in a list of lists "far" and "near"
                    #[[timestamp, rsi, magnitude], [timestamp, rsi, magnitude],...]
                            #calculate magnitude of shift
                            magnitude, nothing, nothing2 = shift_magnitude(data, int(row[0]), half_hour)
                
                            if j == 0:
                                    if magnitude >= 3:
                                            far.append( [ int(row[0]), float(row[2]), magnitude ] ) 
                                    else:
                                            err_ts = str(row[0])
                                            sys.stderr.write('%s far magnitude lower than 3\n' % err_ts)
                            elif j == 1:
                                    if magnitude >= 3:
                                            near.append( [ int(row[0]), float(row[2]), magnitude ] )
                                    else:
                                            err_ts = str(row[0])
                                            sys.stderr.write('%s near magnitude lower than 3\n' % err_ts)
        return far, near, far_data, near_data

def main():
	#compute valid windows
    
    #read raw data and level shifts from files (inputted by user)
    far, near, far_data, near_data = reading_files() 
    if len(far_data) == 0 or len(near_data) == 0:
        sys.stderr.write('far or near data file empty')
        return
    
    #filter rtt data for 5-minute minimums
    far_data = filter_minimums(far_data)
    near_data = filter_minimums(near_data)

    #build initial windows based on level shift input and raw data 
    far_windows = levelshift_windows(far, far_data)
    near_windows = levelshift_windows(near, near_data)
    print "near_windows = " + str(len(near_windows))
    print "far_windows = " + str(len(far_windows))

    #filter far-end windows based on near-side data and windows
    intermediate_windows = window_discarder(far_windows, near_windows, far_data, near_data, 5, False)
    print "intermediate_windows ONE = " + str(len(intermediate_windows))
    #further filter far-end windows for corresponding elevations of near-side rtt
    #NOT detected by level shift
    intermediate_windows = window_discarder(intermediate_windows, intermediate_windows, \
            far_data, near_data, 5, True)
    print "intermediate_windows TWO = " + str(len(intermediate_windows))
    #filter far-end and near-end windows for 90% or more data coverage
    err_string = "far"
    final_windows = uptime_windows(intermediate_windows, far_data, near_data, err_string)
    print "final_windows = " + str(len(final_windows))

    err_string = "near"
    near_windows = uptime_windows(near_windows, near_data, near_data, err_string)

    #store beginning and end of month as a "window"
    monthly_window = [ int( beginnings[month] ) , int( ends[month] ) ] 
    
    #Compute denominator for congestion percentage using month's total minutes and data uptime
    month_minutes = ((monthly_window[1] - monthly_window[0])/60)
    month_uptime = filter_uptime(monthly_window, far_data)
    near_uptime  = filter_uptime(monthly_window, near_data)
    print "far uptime " + str(month_uptime)

    if month_uptime < 5 or near_uptime < 5:
        sys.stderr.write( far_filename )
        sys.stderr.write(' either far or near end uptime less than five percent ')
        return
    month_filtered = month_uptime * month_minutes / 100

    n_month_uptime = filter_uptime(monthly_window, near_data)
    n_month_filtered = n_month_uptime * month_minutes / 100
    #if(congestion_periods > 0): 	
        
    congestion_periods = len(final_windows)
    n_congestion_periods = len(near_windows)
    #Save windows (and far-side separation) to file
    nothing = []
    write_windows(raw_far, final_windows, True, far_data, True, near_data)
    write_windows(raw_near, near_windows, True, near_data, True, nothing)
    write_windows((raw_far + ".intermediate"), far_windows, False, far_data, False, nothing)
    
    #compute congestion metrics
    congestion_minutes, congestion_separation = metrics(final_windows)

    #near-side metrics
    n_congestion_minutes, n_congestion_separation = metrics(near_windows)

    congestion_percentage = round((congestion_minutes/month_filtered),3)

    n_congestion_percentage = round((n_congestion_minutes/n_month_filtered),3)
    if congestion_periods > 0:

        average_minutes = round(float(congestion_minutes)/float(congestion_periods),1)
    
    else:
        average_minutes = 0
    if n_congestion_periods > 0:

        n_average_minutes = round(float(n_congestion_minutes)/float(n_congestion_periods),1)

    else:
        n_average_minutes = 0
    if congestion_periods > 1:
        average_separation = round(float(congestion_separation)/(60*float(congestion_periods-1)),1) #separation in hours
    else:
        average_separation = 0

    if n_congestion_periods > 1:
        n_average_separation = round(float(n_congestion_separation)/(60*float(n_congestion_periods-1)),1) #separation in hours
    else:
        n_average_separation = 0

    #Save congestion metrics to file

    h = open(shifts_file, 'a')

    write_string = ' ' + str(congestion_percentage) + ' ' + str(congestion_minutes) + ' ' \
        + str(congestion_periods) + ' ' + str(average_minutes) + ' ' \
        + str(congestion_separation) + ' ' + str(average_separation)
#        write_string = ' ' + str(congestion_percentage)
    
    h.write(far_filename + write_string)
    h.write('\n')
    write_string = ' ' + str(n_congestion_percentage) + ' ' + str(n_congestion_minutes) + ' ' \
        + str(n_congestion_periods) + ' ' + str(n_average_minutes) + ' ' \
        + str(n_congestion_separation) + ' ' + str(n_average_separation)
    h.write(near_filename + write_string)
    h.write('\n')
    h.close

main()
