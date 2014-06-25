def getAllRows(filenames_to_read, cell_to_lookup = '', col_to_lookup = ''):

    count = 0
    
    for filename in filenames_to_read:
        count = count + getRows(filename, cell_to_lookup, col_to_lookup)

    return count

def getRows(filename_to_read, cell_to_lookup = '', col_to_lookup = ''):
    
    import os
    import csv
    csv.field_size_limit(50000000)

    count = 0
    rfile = open(filename_to_read, 'rbU')
    csv_reader = csv.reader(rfile)
    columns = csv_reader.next()
    csv_reader = csv.DictReader(rfile, columns)

    # read each line of input filename
    for row in csv_reader:
        
        if cell_to_lookup:
            if col_to_lookup:
                # increment if col has cell_to_lookup, else don't
                if row[col_to_lookup] == cell_to_lookup:
                    #print col_to_lookup, row[col_to_lookup]
                    count = count + 1
            else:
                count = count + 1
        else:
            count = count + 1
        
    rfile.close()

    return count

def getUniqueRows(filename_to_read, col_to_lookup):
    
    import cleanup

    return len(cleanup.cleanup(filename_to_read, col_to_lookup))
