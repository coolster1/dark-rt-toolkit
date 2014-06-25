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

                        splits = check_chars.countRtsFile([filename], rtn_type)

                        if rtn_type == 'bool':

                            # the hackiest way to fix an inconvenient return datatype above
                            first_bool = list()
                            second_bool = list()
                            for key in splits:
                                first_bool.append(key)
                                second_bool.append(splits[key])
                            i = 0
                            #third_bool = list()
                            fourth_bool = list()
                            if first_bool[i] == True:
                                #third_bool.append(str(first_bool[i]))
                                fourth_bool.append(second_bool[i])
                                totals[i] = totals[i] + second_bool[i]
                                if len(first_bool) == 2:
                                    #third_bool.append(str(first_bool[i+1]))
                                    fourth_bool.append(second_bool[i+1])
                                    totals[i+1] = totals[i+1] + second_bool[i]
                                else:
                                    #third_bool.append('False')
                                    fourth_bool.append(0)
                            else:
                                # 1st col is False
                                if len(first_bool) == 2:
                                    #third_bool.append(str(first_bool[i+1]))
                                    fourth_bool.append(second_bool[i+1])
                                    totals[i] = totals[i] + second_bool[i+1]
                                else:
                                    #third_bool.append('True')
                                    fourth_bool.append(0)
                                #third_bool.append(str(first_bool[i]))
                                fourth_bool.append(second_bool[i])
                                totals[i+1] = totals[i+1] + second_bool[i]
                            totals[i+2] = totals[i+2] + fourth_bool[i] + fourth_bool[i+1]
                            
                            if display_filename:
                                row_print = "%s\t%s\t%d" % (filename, concat.join(str(x) for x in fourth_bool), fourth_bool[i] + fourth_bool[i+1])
                            else:
                                row_print = "%s\t%d" % (concat.join(str(x) for x in fourth_bool), fourth_bool[i] + fourth_bool[i+1])

                        elif rtn_type == 'three' or rtn_type == 'split':

                            # for rtn_type = three
                            if display_filename:
                                to_print = [filename]
                            else:
                                to_print = list()

                            i = 0
                            total = 0
                            
                            for head in headers:
                                if head != 'filename':
                                    if head == 'total':
                                        to_print.append(total)
                                        totals[i] = totals[i] + total
                                    else:
                                        to_print.append(splits[head] if head in splits else '0')
                                        if head in splits:
                                            total = total + splits[head]
                                            totals[i] = totals[i] + splits[head]
                                        i = i + 1
                            row_print = concat.join(str(x) for x in to_print)

                        print row_print
                        arrays_to_write.append(row_print.split("\t"))

    ### BEGIN grouping all files ###

    if display_filename:
        tab = "TOTAL\t"
    else:
        tab = ""
        print "TOTAL"
        
    row_print = "%s%s" % (tab, concat.join(str(x) for x in totals))

    print row_print
    arrays_to_write.append(row_print.split("\t"))

    if filename_to_write:
        save_csv.SaveCSV(arrays_to_write, filename_to_write)

    ### END grouping all files ###
    
#groupRts("K:\\toolkit_sandbox\\3_copies\\revamp_wed_run", so_far = 70, rtn_type = 'bool', display_filename = True, filename_to_write = "Z:\\phd\\toolkit_revamp\\test.csv")
