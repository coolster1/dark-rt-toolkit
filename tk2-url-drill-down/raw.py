import os.path
import csv
csv.field_size_limit(50000000)
import save_csv

''' Reads two-col raw csv files, meant for cross-block referencing '''
def getRawDict(raw_filename, col_to_lookup, col_to_write):
    
    raw_dict = dict()
    
    if os.path.isfile(raw_filename):
        
        # read raw_filename
        rfile = open(raw_filename, 'rb')
        csv_reader = csv.reader(rfile)
        columns = csv_reader.next()
        csv_reader = csv.DictReader(rfile, columns)
        
        for row in csv_reader:
            raw_dict[row[col_to_lookup]] = row[col_to_write]
            
        rfile.close()

    return raw_dict

''' Reads one-col raw csv files, meant for cross-block referencing '''
def getRawSet(raw_filename, col_to_lookup):
    
    raw_set = set()
    
    if os.path.isfile(raw_filename):
        
        # read raw_filename
        rfile = open(raw_filename, 'rb')
        csv_reader = csv.reader(rfile)
        columns = csv_reader.next()
        csv_reader = csv.DictReader(rfile, columns)
        
        for row in csv_reader:
            raw_set.update(row[col_to_lookup])
            
        rfile.close()

    return raw_set

''' Reads one-col raw csv files, meant for cross-block referencing '''
def getRawList(raw_filename, col_to_lookup):
    
    raw_list = list()
    
    if os.path.isfile(raw_filename):
        
        # read raw_filename
        rfile = open(raw_filename, 'rb')
        csv_reader = csv.reader(rfile)
        columns = csv_reader.next()
        csv_reader = csv.DictReader(rfile, columns)
        
        for row in csv_reader:
            raw_list.append(row[col_to_lookup])
            
        rfile.close()
        
    return raw_list

''' Saves two-col raw csv files, meant for cross-block referencing 
def saveRawDict(dict_to_append, raw_filename, col_to_lookup, col_to_write):

    if dict_to_append:

        import save_csv
        
        for the_id in dict_to_append:
            
            id_pair = [the_id, dict_to_append[the_id]]
            
            if os.path.isfile(raw_filename):
                #append row to end of raw_filename
                fd = open(raw_filename,'ab')
                writer = csv.writer(fd)
                writer.writerow(id_pair)
                fd.close()
                
            else:
                ids_arrays = [[col_to_lookup, col_to_write], id_pair]
                save_csv.SaveCSV(ids_arrays, raw_filename)

 Saves one-col raw csv files, meant for cross-block referencing 
def saveRawSet(set_to_append, raw_filename, col_to_lookup):

    if set_to_append:

        import save_csv
        
        for the_id in set_to_append:

            if os.path.isfile(raw_filename):
                #append row to end of raw_filename
                fd = open(raw_filename,'ab')
                writer = csv.writer(fd)
                writer.writerow([the_id])
                fd.close()
                
            else:
                ids_arrays = [[col_to_lookup], [the_id]]
                save_csv.SaveCSV(ids_arrays, raw_filename)'''

''' Saves two-col raw csv files, meant for cross-block referencing '''
def saveRawDict(dict_to_append, raw_filename, col_to_lookup, col_to_write):

    id_pairs = list()
    if dict_to_append:
        for the_id in dict_to_append:
            id_pairs.append([the_id, dict_to_append[the_id]])
        save_csv.appendCSV(id_pairs, raw_filename, first_row = [col_to_lookup, col_to_write])

''' Saves one-col raw csv files, meant for cross-block referencing '''
def saveRawSet(set_to_append, raw_filename, col_to_lookup):

    the_ids = list()
    if set_to_append:
        for the_id in set_to_append:
            the_ids.append([the_id])
        save_csv.appendCSV(the_ids, raw_filename, first_row = [col_to_lookup])

''' Gets total rows and value of last column of last row '''
def getLastVal(raw_filename, col_to_lookup):

    i = 0
    val = ''
    if os.path.isfile(raw_filename):
        
        # read raw_filename
        rfile = open(raw_filename, 'rb')
        csv_reader = csv.reader(rfile)
        columns = csv_reader.next()
        csv_reader = csv.DictReader(rfile, columns)
        
        for row in csv_reader:
            i = i + 1
            val = row[col_to_lookup]
            
        rfile.close()

    to_return = {'total': i, 'last_val': val}

    return to_return
