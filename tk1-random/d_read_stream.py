import sys
import csv

def readStream(filename_to_read, filename_to_write, friends_dir, id_to_check = ''):
    
    # import packages
    import check_chars
    import save_csv
    csv.field_size_limit(50000000)
    import oauth_req
    import ratelimit
    import operator
    import time

    print "\nReading Stream CSV %s... at %s" % (filename_to_read, time.strftime("%H:%M:%S"))

    # initialize vars
    arrays_to_write = [['url', 'tweet_id', 'tweet_text', 'timestamp', 'author_id', 'proprietary', 'mechanism', 'explicit', 'follower', 'ori_item_id', 'ori_author_id']]
    arrays_to_sort = list()

    # read input filename
    rfile = open(filename_to_read, 'rb')
    csv_reader = csv.reader(rfile)
    columns = csv_reader.next()
    csv_reader = csv.DictReader(rfile, columns)
    i = 0

    # read each line of input filename
    for row in csv_reader:

        if i == 0:
            # print url at beginning
            print "reading %s at %s" % (row['url'], time.strftime("%H:%M:%S"))
        
        elif i % 10 == 0:
            # print timestamp for every 10th row
            print "reading %dth row at %s" % (i, time.strftime("%H:%M:%S"))

        temp_array = [row['url']]

        # classify tweet if id_to_check and row['tweet_id'] matches row
        if id_to_check == row['tweet_id']:
            classify = True
        else:
            classify = False
        #print "the_url: %s, author_id: %s, tweet_id: %s == %s"% (row['url'], row['author_id'], row['tweet_id'], classify)
        inner_row = check_chars.checkCharacteristics(row, friends_dir, classify)
        if inner_row:
            temp_array.extend(inner_row)
            arrays_to_sort.append(temp_array)

        i = i + 1

    rfile.close()

    if arrays_to_sort:

        # sort arrays based on timestamps (col #3)
        sorted_list = sorted(arrays_to_sort, key=operator.itemgetter(3))
        arrays_to_write.extend(sorted_list)

        # save processing output
        save_csv.SaveCSV(arrays_to_write, filename_to_write)

    print "exit read_stream.py at %s" % time.strftime("%H:%M:%S")
