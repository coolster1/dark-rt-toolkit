def groupRts(prefix, so_far, rtn_type = 'bool', display_filename = False, filename_to_write = ''):

    import os
    import check_chars
    import count
    import save_csv

    folders = list()
    i = 1
    while i <= so_far:
        if i < 10:
            folders.append("000%d" % i)
        elif i < 100:
            folders.append("00%d" % i)
        elif i < 1000:
            folders.append("0%d" % i)
        else:
            folders.append("%d" % i)
        i = i + 1

    files = list()
    splits = dict()
    concat = "\t"

    if rtn_type == 'three':
        headers = ['ori', 'visible', 'dark', 'total']

    elif rtn_type == 'split':
        headers = ['ori', 'P@F', 'P@nF', '@F', '@nF',
                   'PRtF', 'PRtnF', 'Rt@F', 'Rt@nF', 'RtF', 'RtnF', 'P@RtF', 'P@RtnF', '@RtF', '@RtnF',
                   'RtF_d', 'RtF_dd', 'RtnF_d', 'RtnF_dd', 'P@F_d', 'P@nF_d', '@F_d', '@nF_d',
                   'OrphanRt', 'Orphan@', 'total']

    elif rtn_type == 'bool':
        headers = ['True', 'False', 'total']

    totals = list()
    for head in headers:
        totals.append(0)
         
    if display_filename:
        temp = ['filename']
        temp.extend(headers)
        headers = temp

    arrays_to_write = [headers]
    
    print concat.join(headers)

    for date in folders:

        if os.path.isdir("%s\\%s\\folg_csv" % (prefix, date)):

            # loop through author_ids
            for the_id in os.listdir("%s\\%s\\folg_csv" % (prefix, date)):

                for url_csv in os.listdir("%s\\%s\\folg_csv\\%s" % (prefix, date, the_id)):

                    filename = "%s\\%s\\folg_csv\\%s\\%s" % (prefix, date, the_id, url_csv)

                    # do preliminary check to see filename's not dupes
                    if os.path.isfile(filename):

                        files.append(filename)

    ### BEGIN grouping all files ###

    if display_filename:
        tab = "%s\t" % prefix
    else:
        tab = ""
    to_print = ""
        
    splits = check_chars.countRtsFile(files, rtn_type)

    if rtn_type == 'bool':
        to_print = "%s%d\t%d\t%d" % (tab, splits[True], splits[False], splits[True] + splits[False])
        print to_print
        arrays_to_write.append(to_print.split('\t'))
        
    elif rtn_type == 'three' or rtn_type == 'split':
        total = 0
        row_print = list()
        for head in headers:
            if head in splits:
                row_print.append(splits[head])
                total = total + splits[head]
            elif head == 'filename':
                row_print.append(tab)
            elif head == 'total':
                row_print.append(total)
        print concat.join(str(x) for x in row_print)
        arrays_to_write.append(row_print)
        
    if filename_to_write:
        save_csv.SaveCSV(arrays_to_write, filename_to_write)

    ### END grouping all files ###
    
#groupRts("K:\\toolkit_sandbox\\3_copies\\redo_wed_run", so_far = 1000, rtn_type = 'bool', display_filename = False, filename_to_write = "Z:\\phd\\toolkit_redo\\test.csv")
