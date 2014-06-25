import b_get_stream
import c_get_search
import d_read_stream
import e_imp_folg
import save_csv
import os
import csv
csv.field_size_limit(50000000)
import time
import raw

################################
## Change to your chosen path ##
################################

prefix = "C:\\toolkit_sandbox\\revamp_mon_run\\"

################################
## Change to your chosen size ##
################################

user_limit = 3000

################################

print "a_main.py started at %s" % time.strftime("%H:%M:%S")

i = 1
authors_seen = set()
user_limit = 3000
while i <= user_limit:

    ### BEGIN prepping data storage ###
    
    if i < 10:
        today = "000%d" % i
    elif i < 100:
        today = "00%d" % i
    elif i < 1000:
        today = "0%d" % i
    else:
        today = "%d" % i
    
    author_urls_dir = "%susers\\" % prefix
        
    # fill in blanks
    if os.path.isdir("%s%s" % (prefix, today)):

        # go to next i
        i = i + 1

    else:

        # this i's folder doesn't exist yet
        
        # get set of authors_seen
        if os.path.isdir("%stimeline" % author_urls_dir):
            # loop through author_urls_dir in timeline
            for the_id in os.listdir("%stimeline" % author_urls_dir):
                if the_id.endswith('.csv') and (the_id.rstrip('.csv') not in authors_seen):
                    authors_seen.add(the_id.rstrip('.csv'))
                    
        ### END prepping data storage ###

        #''
        
        b_get_stream.getStream("%sall_urls.csv" % prefix, "%sdupes.csv" % prefix, author_urls_dir)

        # get first 4 (average mode) URLs
        b_get_stream.getTimelineURLs("%sall_urls.csv" % prefix, author_urls_dir, 4)

        #''
        
        authors_list = list()
        if os.path.isdir("%stimeline" % author_urls_dir):
            # loop through author_urls_dir in timeline
            for the_id in os.listdir("%stimeline" % author_urls_dir):
                if the_id.endswith('.csv') and (the_id.rstrip('.csv') not in authors_seen):
                    authors_list.append(the_id.rstrip('.csv'))
        #'' 

        #print authors_list

        if authors_list:
            
            # keep track of authors seen - all user_limit!
            authors_seen.update(authors_list)

            for author_id in authors_list:

                #'' comment input_urls
                input_urls = raw.getRawList("%stimeline\\%s.csv" % (author_urls_dir, author_id), 'url')

                #'' comment getSearch and store authors' followers
                j = 1
                for the_url in input_urls:
                    if os.path.isfile("%s%s\\streams\\%s\\url_%d.csv" % (prefix, today, author_id, j)) == False:
                        c_get_search.getSearch(the_url,
                                               'recent',
                                               "%s%s\\streams\\%s\\url_%d.csv" % (prefix, today, author_id, j),
                                               "%sfriends" % author_urls_dir)
                    j = j + 1
                #''

                #'' comment readStream
                k = 1
                for the_url in input_urls:
                    if os.path.isfile("%s%s\\streams\\%s\\url_%d.csv" % (prefix, today, author_id, k)) and os.path.isfile("%s%s\\matrix_csv\\%s\\url_%d.csv" % (prefix, today, author_id, k)) == False:
                        d_read_stream.readStream("%s%s\\streams\\%s\\url_%d.csv" % (prefix, today, author_id, k),
                                                 "%s%s\\matrix_csv\\%s\\url_%d.csv" % (prefix, today, author_id, k),
                                                 "%sfriends" % author_urls_dir)
                    k = k + 1
                #''

                l = 1
                for the_url in input_urls:
                    if os.path.isfile("%s%s\\matrix_csv\\%s\\url_%d.csv" % (prefix, today, author_id, l)) and os.path.isfile("%s%s\\folg_csv\\%s\\url_%d.csv" % (prefix, today, author_id, l)) == False:
                        e_imp_folg.impFolg("%s%s\\matrix_csv\\%s\\url_%d.csv" % (prefix, today, author_id, l),
                                           "%s%s\\folg_csv\\%s\\url_%d.csv" % (prefix, today, author_id, l),
                                           "%s\\users\\friends" % prefix)
                    l = l + 1
                #''
                    
                # users counter
                if os.path.isdir("%s%s" % (prefix, today)):
                    i = i + 1
                #'''

        # troubleshooting users counter
        #i = i + 1

    # troubleshooting users counter
    #i = i + 1
