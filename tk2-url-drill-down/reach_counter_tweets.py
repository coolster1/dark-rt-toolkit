import csv
import numpy

keys = ['PRtF', 'PRtnF', 'Rt@F', 'Rt@nF', 'RtF', 'RtnF', 'P@RtF', 'P@RtnF', '@RtF', '@RtnF', 'RtF_d', 'RtnF_d', 'P@F_d', 'P@nF_d', '@F_d', '@nF_d']
vis_keys = ['PRtF', 'PRtnF', 'Rt@F', 'Rt@nF', 'RtF', 'RtnF', 'P@RtF', 'P@RtnF', '@RtF', '@RtnF']
dark_keys = ['RtF_d', 'RtnF_d', 'P@F_d', 'P@nF_d', '@F_d', '@nF_d']

################################
## Change to your chosen path ##
################################

prefix = "C:\\toolkit_sandbox\\revamp_mon_run"

################################

rt_types = dict()
median = dict()
median_vis, median_dark, median_all = list(), list(), list()
rfile = open('%s\\reach_csv\\all.csv' % prefix, 'rb')
csv_reader = csv.reader(rfile)
columns = csv_reader.next()
csv_reader = csv.DictReader(rfile, columns)

for row in csv_reader:
    
    if row['group'] in rt_types:
        to_add = int(row['reach']) - rt_types[row['group']]
        #print "%d - %d = %d" % (int(row['reach']), rt_types[row['group']], to_add)
    else:
        to_add = int(row['reach'])
        #print "to_add: %d" % to_add

    if row['group'] in median:
        median[row['group']].append(to_add)
    else:
        median[row['group']] = [to_add]
        
    rt_types[row['group']] = int(row['reach'])

rfile.close()

for key in keys:
    print "%s\t%d\t%d" % (key, rt_types[key] if key in rt_types else 0, numpy.median(median[key]) if key in median else 0)
    if key in median:
        median_all.extend(median[key])
        if key in vis_keys:
            median_vis.extend(median[key])
        elif key in dark_keys:
            median_dark.extend(median[key])
        else:
            print "error"

print numpy.median(median_vis)

print numpy.median(median_dark)

print numpy.median(median_all)
