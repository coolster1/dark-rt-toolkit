import simplejson
import save_csv
import ratelimit
import time
import raw
import urllib
import os
import oauth_req

def getStream(raw_urls_filename, dupes_filename, author_urls_dir):

    from tweepy.streaming import StreamListener
    from tweepy import OAuthHandler
    from tweepy import Stream
        
    if author_urls_dir:
        author_urls_dir = save_csv.checkDir(author_urls_dir)

    key = ratelimit.getKeys(1)
    consumer_key, consumer_secret, kunci, rahsia = ratelimit.getKeyData(key)

    current_urls = dict()

    seen_urls = raw.getRawSet(raw_urls_filename, 'url')
    newly_seen_urls = set()
    
    class StdOutListener(StreamListener):
        
        """ A listener handles tweets are the received from the stream.
        This is a basic listener that just prints received tweets to stdout.
        """
        def on_status(self, status):
            try:
                # if tweet has a URL
                if status.entities['urls']:

                    # process status
                    #storeStatus(status, raw_status_filename)

                    ##### BEGIN storing author - URLs #####

                    author_id = status.author.id_str
                    
                    for url in status.entities['urls']:

                        the_url = longUntiny(url['expanded_url'])
                        
                        # check if url is a duplicate
                        if (the_url in seen_urls) or (the_url in newly_seen_urls):

                            # save duplicate URL into file
                            save_csv.appendCSV([[the_url]], dupes_filename, ['url'])

                        else:
                            # save URL into author-URL file
                            save_csv.appendCSV([[the_url]], "%s%s.csv" % (author_urls_dir, author_id), ['url'])
                            
                        # store tweet off of Twitter stream
                        search_tweet = [the_url, status.id_str, status.text, author_id]

                        # store the_url and current tweet in dict
                        if the_url in current_urls:
                            current_urls[the_url].append(search_tweet)
                        else:
                            current_urls[the_url] = [search_tweet]
                            newly_seen_urls.add(the_url)

                    ##### END storing author - URLs #####

                    # stop stream
                    return False
                
                else:
                    # continue stream
                    return True

            except Exception, e:
                # Catch any unicode errors while printing to console
                # and just ignore them to avoid breaking application.
                pass

        def on_error(self, status):
            print status

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(kunci, rahsia)

    stream = Stream(auth, l)
    stream.sample()
        
    # save seen current_urls into raw file
    raw.saveRawSet(newly_seen_urls, raw_urls_filename, 'url')
    
def longUntiny(the_url):

    import longurl
    
    expander = longurl.LongURL()
    
    the_url_lu = expander.expandable(the_url)

    if the_url_lu:
        #print "longurl: %s" % expander.expand(the_url, qurl = the_url_lu)
        #lu_ok_ctr = lu_ok_ctr + 1
        the_url = expander.expand(the_url, qurl = the_url_lu)
        
    else:
        #print "------- ERROR %s" % the_url
        #print "------------- try untinyme..."
        
        redo = True
        while redo:
            conn = urllib.urlopen("http://untiny.info/api/1.0/extract?url=%s&format=json" % urllib.quote_plus(the_url))
            ljson2 = simplejson.loads(conn.readline())
            if 'org_url' in ljson2:
                #print "------------- untinyme result: %s" % ljson2['org_url']
                #lu_ok_ctr = lu_ok_ctr + 1
                the_url = ljson2['org_url']
                redo = False
            else:
                if ljson2['error'][1] != 2:
                    #print "------------- ERROR %s: %s" % (ljson2['error'][1], the_url)
                    #print "------------- final result: %s" % the_url
                    #lu_no_ctr = lu_no_ctr + 1
                    redo = False
                '''else:
                    "------------- ERROR Connection failed, retry..."'''
            conn.close()
            
    return the_url

def storeStatus(status, raw_status_filename):

    headers = ['tweet_id', 'tweet_text', 'timestamp', 'author_id', 'rt_status_tweet_id', 'rt_status_user_id', 'ent_mentions_user_id',
               'in_reply_tweet_id', 'in_reply_user_id']
    
    tweet_id = status.id_str
    tweet_text = status.text
    # fix timestamp!
    timestamp = status.created_at
    author_id = status.author.id_str
    ent_mentions_user_id = ''
    
    if status.retweeted_status:
        rt_status_tweet_id = status.retweeted_status.id_str
        rt_status_user_id = status.retweeted_status.user.id_str
    else:
        rt_status_tweet_id = '0'
        rt_status_user_id = '0'
        
    if status.entities['user_mentions']:
        for mtn in status.entities['user_mentions']:
            if ent_mentions_user_id:
                ent_mentions_user_id = ent_mentions_user_id + ',' + mtn['id_str']
            else:
                ent_mentions_user_id = mtn['id_str']
    else:
        ent_mentions_user_id = '0'

    if status.in_reply_to_status_id_str:
        in_reply_tweet_id = status.in_reply_to_status_id_str
    else:
        in_reply_tweet_id = '0'
        
    if status.in_reply_to_user_id_str:
        in_reply_user_id = status.in_reply_to_user_id_str
    else:
        in_reply_user_id = '0'

    data_row = [tweet_id, tweet_text, timestamp, author_id, rt_status_tweet_id, rt_status_user_id, ent_mentions_user_id,
                in_reply_tweet_id, in_reply_user_id]

    save_csv.appendCSV([data_row], raw_status_filename, headers)

def getTimelineURLs(raw_urls_filename, author_urls_dir, limit):

    import csv
    csv.field_size_limit(50000000)
    import cleanup
    from datetime import datetime
    
    if author_urls_dir:
        author_urls_dir = save_csv.checkDir(author_urls_dir)

    # loop through author_urls_dir
    authors_list = list()
    for the_id in os.listdir(author_urls_dir):
        if the_id.endswith('.csv') and os.path.isfile("%stimeline\\%s" % (author_urls_dir, the_id)) == False:
            authors_list.append(the_id.rstrip('.csv'))
    
    seen_overall_urls = set(raw.getRawList(raw_urls_filename, 'url'))
    
    for author_id in authors_list:

        print "getting timeline URLs for author %s at %s" % (author_id, time.strftime("%H:%M:%S"))

        # look for author's presaved list of URLs
        current_urls = raw.getRawList("%s%s.csv" % (author_urls_dir, author_id), 'url')
        current_urls_set = set(current_urls)
        current_urls = [current_urls]

        # get author_id's user timeline
        key = ratelimit.getKeys(1, 'timeline')
        consumer_key, consumer_secret, kunci, rahsia = ratelimit.getKeyData(key)
        url = "https://api.twitter.com/1.1/statuses/user_timeline.json?user_id=%s&count=200?trim_user=true" % (author_id)

        api_response = oauth_req.OauthReq(url, consumer_key, consumer_secret, kunci, rahsia)

        if api_response[0]['status'] == '200' and api_response[1]:
            
            json_response = simplejson.loads(api_response[1])

            for tweet in json_response:
                if len(current_urls) < limit:
                    tdelta = datetime.now() - datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
                    if tdelta.days < 1:
                        if 'urls' in tweet['entities'] and tweet['entities']['urls']:
                            for url_dict in tweet['entities']['urls']:
                                try:
                                    # expand each URL found
                                    ent_url = longUntiny(url_dict['expanded_url']).decode("utf-8")
                                    if (ent_url not in seen_overall_urls) and (ent_url not in current_urls_set):
                                        # store each tweet
                                        current_urls.append([ent_url])
                                        current_urls_set.add(ent_url)
                                except:
                                    print 'non-ascii chars'
                            else:
                                    #print "limit reached!"
                                    break
                    else:
                        #print "over 1 day!"
                        break
                else:
                    break
                          
        elif api_response[0]['status'] == '429':

            sleep_time = 30
            print "rate limited at %s! neet to wait %d s" % (time.strftime("%H:%M:%S"), sleep_time)
            time.sleep(sleep_time)
            print "woken up at %s!" % time.strftime("%H:%M:%S")
            
        else:

            print "error getting user %s's timeline: %s" % (author_id, api_response[0]['status'])
           
        # store author's user timeline's URLs
        save_csv.appendCSV(current_urls, "%stimeline\\%s.csv" % (author_urls_dir, author_id), ['url'])
        raw.saveRawSet(current_urls_set, raw_urls_filename, 'url')

        # store author's friends list
        #storeFriends(author_id, "%sfriends" % author_urls_dir)

    # do cleanup - for some reason pattern matching above still is letting dupes in

    # cleanup all_urls file
    cleanup.cleanupOneCol(raw_urls_filename, 'url')
