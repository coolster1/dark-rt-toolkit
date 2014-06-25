import os

def cleanup(filename_to_read, col_to_lookup, delete = False, row_to_del = ''):
    
    import csv

    # cleanup all_urls file
    arrays_to_write = [[col_to_lookup]]
    rfile = open(filename_to_read, 'rb')
    csv_reader = csv.reader(rfile)
    columns = csv_reader.next()
    csv_reader = csv.DictReader(rfile, columns)

    seen = set()
    for row in csv_reader:
        if row[col_to_lookup] not in seen:
            if delete == '' or (delete and row_to_del != row[col_to_lookup]):
                arrays_to_write.append([row[col_to_lookup]])
                seen.add(row[col_to_lookup])
        
    rfile.close()

    return arrays_to_write

def cleanupOneCol(filename_to_read, col_to_lookup, filename_to_write = '', delete = False, row_to_del = ''):

    import save_csv

    arrays_to_write = cleanup(filename_to_read, col_to_lookup, delete, row_to_del)
    
    if filename_to_write:
        # don't overwrite old file
        save_csv.SaveCSV(arrays_to_write, filename_to_write)
    else:
        # overwrite old file
        os.remove(filename_to_read)
        save_csv.SaveCSV(arrays_to_write, filename_to_read)
        
def cleanupImpFolg(prefix, today, author_id, i):

    if os.path.isfile("%s\\%s\\folg_csv\\%s\\url_%d.csv" % (prefix, today, author_id, i)) == False:
        if os.path.isfile("%s\\%s\\folg_csv\\%s\\url_%d_seen_friends.csv" % (prefix, today, author_id, i)):
            os.remove("%s\\%s\\folg_csv\\%s\\url_%d_seen_friends.csv" % (prefix, today, author_id, i))
    print "cleanupImpFolg done!"
