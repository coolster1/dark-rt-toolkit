def impFolg(filename_to_read, filename_to_write, friends_dir):#, raw_filename_friends):
    
    import time
    import csv
    csv.field_size_limit(50000000)
    import raw
    import cleanup
    import save_csv
    
    print "\nGetting implicit followers for file %s... at %s" % (filename_to_read, time.strftime("%H:%M:%S"))

    arrays_to_write = [['url', 'tweet_id', 'tweet_text', 'timestamp', 'author_id', 'proprietary', 'mechanism', 'explicit', 'follower',
                        'ori_item_id', 'ori_author_id']]

    authors_seen = set()
    
    # read filename_to_read
    rfile = open(filename_to_read, 'rb')
    csv_reader = csv.reader(rfile)
    columns = csv_reader.next()
    csv_reader = csv.DictReader(rfile, columns)
    
    for row in csv_reader:

        author_id = row['author_id']
        mechanism = row['mechanism']
        follower = row['follower']
        ori_author_id = row['ori_author_id']
    
        # check if author was once a follower
        if 'Rt' not in mechanism:
            
            authors_friends = set(raw.getRawList("%s\\%s.csv" % (friends_dir, author_id), 'friends'))
            find_a_friend = authors_seen & authors_friends
            
            if find_a_friend:
                # follow current mechanism, unless it's ori, so default to Rt
                mechanism = "Imp%s" % (row['mechanism'] if row['mechanism'] != '0' else "Rt")
                if 'Rt' in mechanism:
                    # implicitly following someone, so force follower = True
                    follower = '1'
                # store implicit friends
                if ori_author_id == '0':
                    temp_author_id = ''
                    for frn in find_a_friend:
                        if temp_author_id:
                            temp_author_id = temp_author_id + ',' + frn
                        else:
                            temp_author_id = frn
                    ori_author_id = temp_author_id
                    
        arrays_to_write.append([row['url'], row['tweet_id'], row['tweet_text'], row['timestamp'], author_id, row['proprietary'], mechanism,
                                row['explicit'], follower, row['ori_item_id'], ori_author_id])
    
        authors_seen.add(author_id)

    rfile.close()

    # store new matrix csvs
    if len(arrays_to_write) > 1:
        save_csv.SaveCSV(arrays_to_write, filename_to_write)
    
    print "exit imp_folg.py at %s" % time.strftime("%H:%M:%S")
