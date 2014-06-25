def getRows(filename_to_read):
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
        count = count + 1
    rfile.close()

    return count
