import os
import count

prefix = "C:\\toolkit_sandbox\\redo_mon_run"
folders = list()
i = 1
so_far = 20000
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
all_users = 0

for date in folders:

    if os.path.isdir("%s\\%s\\folg_csv" % (prefix, date)):

        all_users = all_users + len(os.listdir("%s\\%s\\folg_csv" % (prefix, date)))
        
        # loop through author_ids
        for the_id in os.listdir("%s\\%s\\folg_csv" % (prefix, date)):

            breaker = 0
            i = 1
            
            while breaker < 2000:
                if os.path.isfile("%s\\%s\\folg_csv\\%s\\url_%d.csv" % (prefix, date, the_id, i)):

                    # store filenames
                    files.append("%s\\%s\\folg_csv\\%s\\url_%d.csv" % (prefix, date, the_id, i))

                    i = i + 1
                    
                else:
                    breaker = breaker + 1

'''import pprint
pprint.pprint(files)
#'''
all_tweets = count.getAllRows(files)
ori_tweets = count.getAllRows(files, '0', 'mechanism')
rt_tweets = count.getAllRows(files, 'Rt', 'mechanism')
at_tweets = count.getAllRows(files, '@', 'mechanism')
imprt_tweets = count.getAllRows(files, 'ImpRt', 'mechanism')
impat_tweets = count.getAllRows(files, 'Imp@', 'mechanism')
rtat_tweets = count.getAllRows(files, 'Rt@', 'mechanism')
atrt_tweets = count.getAllRows(files, '@Rt', 'mechanism')
print "All tweets: %d, Ori: %d, Rts: %d, @: %d, ImpRts: %d, Imp@: %d, Rt@: %d, @Rt: %d, Users: %d" % (all_tweets, ori_tweets, rt_tweets, at_tweets,
                                                                                                      imprt_tweets, impat_tweets, rtat_tweets, atrt_tweets,
                                                                                                      all_users)
