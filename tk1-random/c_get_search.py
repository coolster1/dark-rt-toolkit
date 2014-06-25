import time
import oauth_req
import simplejson
import ratelimit

def getSearch(query, search_type, filename_to_write, friends_dir, count = 100):
    
    # import packages
    import csv
    import urllib
    import save_csv

    # get Twitter API keys
    key = ratelimit.getKeys(1)
    consumer_key, consumer_secret, kunci, rahsia = ratelimit.getKeyData(key)

    next_page = ''
    loop = True

    headers = ['url', 'tweet_id', 'tweet_text', 'timestamp', 'author_id', 'rt_status_tweet_id', 'rt_status_user_id', 'ent_mentions_user_id',
               'in_reply_tweet_id', 'in_reply_user_id']

    seen_ids = set()
    
    while loop == True:
        
        #print consumer_key, consumer_secret, kunci, rahsia
        if next_page:
            url2 = "https://api.twitter.com/1.1/search/tweets.json%s" % (next_page)
            next_page = ''
        else:
            url2 = "https://api.twitter.com/1.1/search/tweets.json?q=%s&result_type=%s&count=%d" % (query, search_type, count)

        api_response2 = oauth_req.OauthReq(url2, consumer_key, consumer_secret, kunci, rahsia)

        if api_response2[0]['status'] == '200' and api_response2[1]:

            arrays_to_write = list()

            json_response = simplejson.loads(api_response2[1])
            
            for status in json_response['statuses']:

                # parse status
                new_status = parseStatus(status)
                new_status_q = [query]
                new_status_q.extend(new_status)

                if new_status and new_status_q:
                    arrays_to_write.append(new_status_q)

                    # store friends for each tweet's author if not taken yet
                    #storeFriends(new_status_q[4], friends_dir)

            if arrays_to_write:
                save_csv.appendCSV(arrays_to_write, filename_to_write, headers)
            else:
                print "no tweets for %s" % query

            if 'next_results' in json_response['search_metadata']:
                next_page = json_response['search_metadata']['next_results']
                '''print "next_page: %s" % next_page
            else:
                print "no more next_page"
            #print i, next_page'''
            
        elif api_response2[0]['status'] == '403' and api_response2[1]:
            
            json_response = simplejson.loads(api_response2[1])
            print "error = %s" % (json_response['error'])

        if next_page == '':
            loop = False

def parseStatus(status):

    parsed_status = list()
    tweet_id = status['id_str']
    tweet_text = status['text'].encode('utf-8')
    timestamp = time.strftime('%d-%m-%Y %H:%M:%S', time.strptime(status['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
    author_id = status['user']['id_str']
    ent_mentions_user_id = ''
    if ('retweeted_status' in status) and status['retweeted_status']:
        rt_status_tweet_id = status['retweeted_status']['id_str']
        rt_status_user_id = status['retweeted_status']['user']['id_str']
    else:
        rt_status_tweet_id = '0'
        rt_status_user_id = '0'
        
    if status['entities']['user_mentions']:
        for mtn in status['entities']['user_mentions']:
            if ent_mentions_user_id:
                ent_mentions_user_id = ent_mentions_user_id + ',' + mtn['id_str']
            else:
                ent_mentions_user_id = mtn['id_str']
    else:
        ent_mentions_user_id = '0'

    if status['in_reply_to_status_id_str']:
        in_reply_tweet_id = status['in_reply_to_status_id_str']
    else:
        in_reply_tweet_id = '0'
        
    if status['in_reply_to_user_id_str']:
        in_reply_user_id = status['in_reply_to_user_id_str']
    else:
        in_reply_user_id = '0'

    parsed_status = [tweet_id, tweet_text, timestamp, author_id, rt_status_tweet_id, rt_status_user_id, ent_mentions_user_id,
                     in_reply_tweet_id, in_reply_user_id]
    
    return parsed_status

def storeFriends(auth_id, friends_dir):

    import os
    import raw
    
    # look for auth's total friends
    if os.path.isfile("%s\\%s.csv" % (friends_dir, auth_id)) == False:

        print "getting friends for auth: %s at %s" % (auth_id, time.strftime("%H:%M:%S"))
        # haven't seen this auth yet, so grab friends list
        current_friends = set()
        cursor = "-1"
        exit_loop = False

        # paginating loop
        while exit_loop == False:

            # only proceed if there's no rate limiting
            key = ratelimit.getKeys(1, f = 'friends')
            consumer_key, consumer_secret, kunci, rahsia = ratelimit.getKeyData(key)

            # get followers for auth_id
            url = 'https://api.twitter.com/1.1/friends/ids.json?user_id=%s&cursor=%s&stringify_ids=true' % (auth_id, cursor)
            
            api_response = oauth_req.OauthReq(url, consumer_key, consumer_secret, kunci, rahsia)

            if api_response[0]['status'] == '200' and api_response[1]:
                
                json_response = simplejson.loads(api_response[1])

                if json_response['ids']:
                    
                    if current_friends:
                        # if paginating
                        current_friends.update(set(json_response['ids']))
                    else:
                        # on 1st block, no paginating yet
                        current_friends = set(json_response['ids'])

                    if json_response['next_cursor_str'] != '0':
                        # if paginating, change cursor to next page
                        cursor = json_response['next_cursor_str']
                    else:
                        # no more IDs, break loop
                        exit_loop = True
                else:
                    # no followers
                    exit_loop = True
                                        
            else:

                # error, or not authorized (protected)
                exit_loop = True

        # store followers for auth_id
        raw.saveRawSet(current_friends, "%s\\%s.csv" % (friends_dir, auth_id), "friends")
