import os
import count
import raw

prefix = "C:\\toolkit_sandbox\\revamp_mon_run"
folders = list()
i = 1
so_far = 2000
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
all_urls = 0
single = 0

unique_users = set()
unique_tweets = set()
unique_urls = set()

dupes_tweets = 0
dupes_urls = 0

print "All\tOri\tRts\t@\tImpRts\tImp@\tRt@\t@Rt\tImp@Rt\tImpRt@"

for date in folders:

    if os.path.isdir("%s\\%s\\folg_csv" % (prefix, date)):

        # loop through author_ids
        for the_id in os.listdir("%s\\%s\\folg_csv" % (prefix, date)):

            for url_csv in os.listdir("%s\\%s\\folg_csv\\%s" % (prefix, date, the_id)):

                filename = "%s\\%s\\folg_csv\\%s\\%s" % (prefix, date, the_id, url_csv)

                # do preliminary check to see filename's not dupes
                if os.path.isfile(filename):

                    all_tweets = count.getRows(filename)
                    
                    if all_tweets < 1:

                        # filename has 0 tweets, so delete folder
                        os.remove(filename)

                    else:
                        
                        # store unique tweet and url if not seen
                        tweet_url_dict = raw.getRawDict("%s\\%s\\folg_csv\\%s\\%s" % (prefix, date, the_id, url_csv), 'tweet_id', 'url')

                        new_url = True

                        for tweet in tweet_url_dict:
                            
                            if tweet not in unique_tweets:
                                unique_tweets.add(tweet)
                                if new_url:
                                    if tweet_url_dict[tweet] in unique_urls:
                                        #print "url %s already seen: %s" % (tweet_url_dict[tweet], filename)
                                        dupes_urls = dupes_urls + 1
                                    new_url = False
                                unique_urls.add(tweet_url_dict[tweet])
                            else:
                                #print "tweet %s has already been seen: %s" % (tweet, filename)
                                dupes_tweets = dupes_tweets + 1
                                # tweet already seen, so delete folder
                                if os.path.isfile(filename):
                                    os.remove(filename)
                                
                # check if filename still exists after cleanup
                if os.path.isfile(filename):

                    # store filenames
                    files.append(filename)
       
                    # store unique user if not seen
                    unique_users.add(the_id)
                
                    ori_tweets = count.getRows(filename, '0', 'mechanism')
                    rt_tweets = count.getRows(filename, 'Rt', 'mechanism')
                    at_tweets = count.getRows(filename, '@', 'mechanism')
                    imprt_tweets = count.getRows(filename, 'ImpRt', 'mechanism')
                    impat_tweets = count.getRows(filename, 'Imp@', 'mechanism')
                    rtat_tweets = count.getRows(filename, 'Rt@', 'mechanism')
                    atrt_tweets = count.getRows(filename, '@Rt', 'mechanism')
                    impatrt_tweets = count.getRows(filename, 'Imp@Rt', 'mechanism')
                    imprtat_tweets = count.getRows(filename, 'ImpRt@', 'mechanism')
                    print "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d" % (all_tweets, ori_tweets, rt_tweets, at_tweets,
                                                                      imprt_tweets, impat_tweets, rtat_tweets, atrt_tweets,
                                                                      impatrt_tweets, imprtat_tweets)
                    if imprt_tweets > 40:
                        print filename
                        
                    if all_tweets == 1:
                        single = single + 1

                    all_urls = all_urls + 1
     
'''import pprint
pprint.pprint(files)
'''
all_tweets = count.getAllRows(files)
ori_tweets = count.getAllRows(files, '0', 'mechanism')
rt_tweets = count.getAllRows(files, 'Rt', 'mechanism')
at_tweets = count.getAllRows(files, '@', 'mechanism')
imprt_tweets = count.getAllRows(files, 'ImpRt', 'mechanism')
impat_tweets = count.getAllRows(files, 'Imp@', 'mechanism')
rtat_tweets = count.getAllRows(files, 'Rt@', 'mechanism')
atrt_tweets = count.getAllRows(files, '@Rt', 'mechanism')
impatrt_tweets = count.getAllRows(files, 'Imp@Rt', 'mechanism')
imprtat_tweets = count.getAllRows(files, 'ImpRt@', 'mechanism')
print "Total"
print "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d" % (all_tweets, ori_tweets, rt_tweets, at_tweets, imprt_tweets, impat_tweets,
                                                  rtat_tweets, atrt_tweets, impatrt_tweets, imprtat_tweets)
# check for dupes
#print "Dupes tweets: %d, urls: %d" % (dupes_tweets, dupes_urls)

print "URLs - single tweets: %d" % single

print "Unique - users: %d, tweets: %d, URLs: %d" % (len(unique_users), len(unique_tweets), len(unique_urls))
