import os

def SaveCSV(array_to_write, output_filename):

    #initialize variables
    i = 1

    ensureDir(output_filename)
    root_filename = stripCSV(output_filename)
    
    # check filename doesn't exist
    csv_filename = root_filename
    while os.path.exists(csv_filename + '.csv'):
        print csv_filename + '.csv already exists. Filename changed...'
        i += 1
        csv_filename = root_filename + str(i)

    # write array into CSV file
    from csv import writer
    
    csv_filename = csv_filename + '.csv'
    wfile = open(csv_filename, 'wb')
    the_writer = writer(wfile)
    the_writer.writerows(array_to_write)
    wfile.close()

    print "File created: %s" % csv_filename

def ensureDir(filename):

    i = filename.rfind("\\")
    dir_name = filename[:i]

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        print "Directory %s created" % dir_name

def checkDir(filename):

    if filename.endswith("\\"):
        rtn_filename = filename
    else:
        rtn_filename = "%s\\" % filename
        
    return rtn_filename

def stripCSV(strip_filename):
    
    # strip '.csv' off filenames
    if strip_filename.endswith('.csv'):
        rtn_filename = strip_filename.rsplit('.csv')[0]
    else:
        rtn_filename = strip_filename
        
    return rtn_filename

def appendCSV(arrays_to_append, append_filename, first_row):

    import csv
    
    if os.path.isfile(append_filename):
        #append row to end of append_filename
        fd = open(append_filename,'ab')
        writer = csv.writer(fd)
        writer.writerows(arrays_to_append)
        fd.close()
        print "File appended: %s" % append_filename
        
    else:
        arrays_to_write = [first_row]
        arrays_to_write.extend(arrays_to_append)
        SaveCSV(arrays_to_write, append_filename)

def sortCSV(filename_to_read):

    # import packages
    import csv
    csv.field_size_limit(50000000)
    import operator

    # read input filename
    rfile = open(filename_to_read, 'rb')
    csv_reader = csv.reader(rfile)
    columns = csv_reader.next()
    
    sortedlist = sorted(csv_reader, key=operator.itemgetter(3))
    arrays_to_write = [columns]
    arrays_to_write.extend(sortedlist)
    rfile.close()

    return arrays_to_write
