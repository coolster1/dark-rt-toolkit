import os

def checkCharacteristics(row, friends_dir, classify = False):

    #print "\nChecking characteristics of tweets..."

    # import packages
    import oauth_req
    import simplejson
    import string
    import save_csv
    import time
    import ratelimit
    import raw

    # initialize vars
    all_rts = ["rt @", "rt@", "rt:@", "rt: @" , "retweet @", "via @", "retweet :@", "r/t", "rt:",
               "RT @", "RT@", "RT:@", "RT: @" , "RETWEET @", "VIA @", "RETWEET :@", "R/T", "RT:",
               "rt .@", "rt: .@" , "retweet .@", "via .@",
               "RT .@", "RT: .@" , "RETWEET .@", "VIA .@",
               " RT.", " rt.", "#RT", "#rt", " RT ", " rt ",
               "mt @", "mt@", "mt:@", "mt: @" , "m/t", "mt:",
               "MT @", "MT@", "MT:@", "MT: @" , "M/T", "MT:",
               "mt .@", "mt: .@" ,
               "MT .@", "MT: .@" ,
               " MT.", " mt.", "#MT", "#mt", " MT ", " mt ",
               "ht @", "ht@", "ht:@", "ht: @" , "h/t", "ht:",
               "HT @", "HT@", "HT:@", "HT: @" , "H/T", "HT:",
               "ht .@", "ht: .@" ,
               "HT .@", "HT: .@" ,
               " HT.", " ht.", "#HT", "#ht", " HT ", " ht "]
    
    tweet_id = row['tweet_id']
    tweet_text = row['tweet_text']
    timestamp = row['timestamp']
    author_id = row['author_id']
    proprietary = False
    mechanism = None
    explicit = False
    follower = False
    ori_tweet_id = None
    ori_author_id = None
    users_mentioned = list()

    # check if need to classify this tweet or not
    if classify == True:
        
        if friends_dir:
            friends_dir = save_csv.checkDir(friends_dir)

        if row['rt_status_tweet_id'] != '0':
            
            # This is a native Rt
            
            proprietary = True
            ori_tweet_id = row['rt_status_tweet_id']
            explicit = True
            mechanism = "Rt"

            if row['rt_status_user_id'] != '0':
                ori_author_id = row['rt_status_user_id']
            '''else:
                ori_author_id = row['ent_mentions_user_id']'''
                        
        elif tweet_text[0:2] == 'RT' or tweet_text[0:2] == 'rt' or tweet_text[0:3] == 'RT ' or tweet_text[0:3] == 'rt ' or tweet_text[0:2] == '.@':
            
            mechanism = "Rt"
            explicit = True
            
            if row['in_reply_tweet_id'] != '0' and row['in_reply_user_id'] != '0':
                ori_tweet_id = row['in_reply_tweet_id']
                ori_author_id = row['in_reply_user_id']

        else:

            # Check if tweet text contains "RT/via/HT/MT"

            for rt_syntax in all_rts:

                if rt_syntax in tweet_text:
                    
                    mechanism = "Rt"
                    explicit = True
                    
                    if row['in_reply_tweet_id'] != '0' and row['in_reply_user_id'] != '0':
                        ori_tweet_id = row['in_reply_tweet_id']
                        ori_author_id = row['in_reply_user_id']
                        
        if tweet_text[0] == '@' and  tweet_text[1] != ' ':

            # Check if tweet is a reply
            
            if mechanism == None:
                mechanism = "@"
            elif mechanism == "Rt":
                mechanism = "@Rt"
            
            explicit = True
            
            if row['in_reply_tweet_id'] != '0' and row['in_reply_user_id'] != '0':
                
                proprietary = True
                    
                if ori_tweet_id == None:
                    ori_tweet_id = row['in_reply_tweet_id']
                else:
                    print "new ori_tweet_id %s already filled w %s" % (row['in_reply_tweet_id'], ori_tweet_id)
                    
                if ori_author_id == None:
                    ori_author_id = row['in_reply_user_id']
                else:
                    print "new ori_author_id %s already filled w %s" % (row['in_reply_user_id'], ori_author_id)

        if proprietary == True and mechanism == "Rt":
            
            # Check if author is following originator

            if author_id != ori_author_id:

                # if author_id's friends list is available, lookup that first
                if os.path.isfile("%s%s.csv" % (friends_dir, author_id)):
                    # friends list already stored
                    print "@ getting friends list.. %s%s.csv" % (friends_dir, author_id)
                    current_friends = raw.getRawList("%s%s.csv" % (friends_dir, author_id), "friends")
                    #print current_friends

                    if ori_author_id in current_friends:
                        follower = True
                        
                else:
                    
                    # if unavailable, lookup API
                    
                    # only proceed if there's no rate limiting
                    key = ratelimit.getKeys(1, f = 'friendships')
                    consumer_key, consumer_secret, kunci, rahsia = ratelimit.getKeyData(key)

                    url2 = 'https://api.twitter.com/1.1/friendships/show.json?source_id=%s&target_id=%s' % (ori_author_id, author_id)
                    api_response2 = oauth_req.OauthReq(url2, consumer_key, consumer_secret, kunci, rahsia)

                    if api_response2[0]['status'] == '200' and api_response2[1]:

                        json_response2 = simplejson.loads(api_response2[1])

                        if follower == False:
                            follower = json_response2['relationship']['target']['following']
                        
                    elif api_response2[0]['status'] == '403' and api_response2[1]:
                        
                        json_response2 = simplejson.loads(api_response2[1])
                        print "follow_ori: tweet_id = %s, author_id = %s, ori_author_id = %s" % (tweet_id, author_id, ori_author_id)
                        print "error: %s" % json_response2['errors'][0]['message']

        if (row['ent_mentions_user_id'] != '0') and ori_author_id == None:

            # Check if user mentions exists in tweet text

            # grab all users mentioned in tweet text
            users_mentioned = row['ent_mentions_user_id'].split(',')

            for mention in users_mentioned:

                # Check if author is following mentioned user
                    
                if author_id != mention:

                    # if author_id's friends list is available, lookup that first
                    if os.path.isfile("%s%s.csv" % (friends_dir, author_id)):
                        # friends list already stored
                        print "@ getting friends list.. %s%s.csv" % (friends_dir, author_id)
                        current_friends = raw.getRawList("%s%s.csv" % (friends_dir, author_id), "friends")
                        #print current_friends

                        if (mention in current_friends) and (follower == False):
                            follower = True
                            
                    else:
                        
                        # if unavailable, lookup API

                        # only proceed if there's no rate limiting
                        key = ratelimit.getKeys(1, f = 'friendships')
                        consumer_key, consumer_secret, kunci, rahsia = ratelimit.getKeyData(key)

                        url3 = 'https://api.twitter.com/1.1/friendships/show.json?source_id=%s&target_id=%s' % (mention, author_id)
                        api_response3 = oauth_req.OauthReq(url3, consumer_key, consumer_secret, kunci, rahsia)

                        if api_response3[0]['status'] == '200' and api_response3[1]:

                            json_response3 = simplejson.loads(api_response3[1])

                            if follower == False:
                                follower = json_response3['relationship']['target']['following']
                                
                        elif api_response3[0]['status'] == '403' and api_response3[1]:
                            
                            json_response3 = simplejson.loads(api_response3[1])
                            print "follow_mention: tweet_id = %s, author_id = %s, mention_id = %s" % (tweet_id, author_id, mention)
                            print "error: %s" % json_response3['errors'][0]['message']
                
            ori_author_id = row['ent_mentions_user_id']

        if proprietary == False:

            if ori_tweet_id:

                # Check if tweet is an RT created from a proprietary reply
                mechanism = "Rt@"

            elif mechanism == 'Rt' and ori_author_id == None and ori_tweet_id == None and follower == False and explicit:

                # misclassified Rt - fix this
                explicit = False
                mechanism = None
            
    return [tweet_id,
            tweet_text,
            timestamp,
            author_id,
            '1' if proprietary else '0',
            mechanism if mechanism else '0',
            '1' if explicit else '0',
            '1' if follower else '0',
            ori_tweet_id if ori_tweet_id else '0',
            ori_author_id if ori_author_id else '0']

def isRt(row, rtn_type = 'bool'):

    isRt = False
    rt_type = 'ori'
    
    if row['tweet_text'] != 'removed' and row['tweet_text'] != 'File not found.' and row['mechanism'] != '0':
        proprietary = True if (row['proprietary'] == '1') else False
        mechanism = row['mechanism']
        explicit = True if (row['explicit'] == '1') else False
        follower = True if (row['follower'] == '1') else False
        ori_item_id = False if (row['ori_item_id'] == '0') else True
        ori_author_id = False if (row['ori_author_id'] == '0') else True

        if (proprietary and (mechanism == 'Rt') and explicit and follower and ori_item_id and ori_author_id):
            #print "this is PRtF! 11001111"
            isRt = True
            rt_type = "PRtF"
            
        elif (proprietary and (mechanism == 'Rt') and explicit and (follower == False) and ori_item_id and ori_author_id):
            #print "this is PRtnF! 11001011"
            isRt = True
            rt_type = "PRtnF"
            
        elif (proprietary == False and (mechanism == 'Rt@') and explicit and follower and ori_item_id and ori_author_id):
            #print "this is Rt@F! 01001111"
            isRt = True
            rt_type = "Rt@F"

        elif (proprietary == False and (mechanism == 'Rt@') and explicit and (follower == False) and ori_item_id and ori_author_id):
            #print "this is Rt@nF! 01001011"
            isRt = True
            rt_type = "Rt@nF"

        elif ((proprietary == False) and (mechanism == 'Rt') and explicit and follower and (ori_item_id == False) and ori_author_id):
            #print "this is RtF! 01001101"
            isRt = True
            rt_type = "RtF"
        elif ((proprietary == False) and (mechanism == 'Rt') and explicit and follower and (ori_item_id == False) and (ori_author_id == False)):
            #print "this is RtF! 01001100"
            isRt = True
            rt_type = "RtF"

        elif ((proprietary == False) and ((mechanism == 'Rt') or (mechanism == 'ImpRt')) and (explicit == False) and follower and (ori_item_id == False)):
            #print "this is RtF_d! 01000101 or 01000100"
            isRt = True
            rt_type = "RtF_d"
            
        elif ((proprietary == False) and (mechanism == 'Rt') and explicit and (follower == False) and (ori_item_id == False) and ori_author_id):
            #print "this is RtnF! 01001001"
            isRt = True
            rt_type = "RtnF"
        elif ((proprietary == False) and (mechanism == 'Rt') and explicit and (follower == False) and (ori_item_id == False) and (ori_author_id == False)):
            #print "this is RtnF! 01001000"
            isRt = True
            rt_type = "RtnF"

        elif ((proprietary == False) and ((mechanism == 'Rt') or (mechanism == 'ImpRt')) and (explicit == False) and (follower == False) and (ori_item_id == False)):
            #print "this is RtnF_d! 01000001 or 01000000"
            isRt = True
            rt_type = "RtnF_d"
            
        elif (proprietary and (mechanism == '@') and explicit and follower and ori_item_id and ori_author_id):
            #print "this is P@F! 10101111"
            isRt = False
            rt_type = "P@F"
            
        elif (proprietary and (mechanism == 'Imp@') and explicit and follower and ori_item_id and ori_author_id):
            #print "this is P@F_d! 10101111"
            isRt = True
            rt_type = "P@F_d"
            
        elif (proprietary and (mechanism == '@') and explicit and (follower == False) and ori_item_id and ori_author_id):
            #print "this is P@nF! 10101011"
            isRt = False
            rt_type = "P@nF"
            
        elif (proprietary and (mechanism == 'Imp@') and explicit and (follower == False) and ori_item_id and ori_author_id):
            #print "this is P@nF_d! 10101011"
            isRt = True
            rt_type = "P@nF_d"
            
        elif (proprietary and (mechanism == '@Rt') and explicit and follower and ori_item_id and ori_author_id):
            #print "this is P@RtF! 11101111"
            isRt = True
            rt_type = "P@RtF"
            
        elif (proprietary and (mechanism == '@Rt') and explicit and (follower == False) and ori_item_id and ori_author_id):
            #print "this is P@RtnF! 11101011"
            isRt = True
            rt_type = "P@RtnF"
            
        elif ((proprietary == False) and (mechanism == '@') and explicit and follower and (ori_item_id == False) and ori_author_id):
            #print "this is @F! 00101101"
            isRt = False
            rt_type = "@F"
            
        elif ((proprietary == False) and (mechanism == 'Imp@') and explicit and follower and (ori_item_id == False) and ori_author_id):
            #print "this is @F_d! 00101101"
            isRt = True
            rt_type = "@F_d"
            
        elif ((proprietary == False) and (mechanism == '@') and explicit and (follower == False) and (ori_item_id == False) and ori_author_id):
            #print "this is @nF! 00101001"
            isRt = False
            rt_type = "@nF"

        elif ((proprietary == False) and (mechanism == 'Imp@') and explicit and (follower == False) and (ori_item_id == False) and ori_author_id):
            #print "this is @nF_d! 00101001"
            isRt = True
            rt_type = "@nF_d"

        elif ((proprietary == False)and (mechanism == '@Rt') and explicit and follower and (ori_item_id == False) and ori_author_id):
            #print "this is @RtF! 01101101"
            isRt = True
            rt_type = "@RtF"
            
        elif ((proprietary == False) and (mechanism == '@Rt') and explicit and (follower == False) and (ori_item_id == False) and ori_author_id):
            #print "this is @RtnF! 01101001"
            isRt = True
            rt_type = "@RtnF"
            
        elif (proprietary and (mechanism == 'Rt') and explicit and (follower == False) and ori_item_id and (ori_author_id == False)):
            #print "this is OrphanRt! 11001010"
            isRt = True
            rt_type = "OrphanRt"
            
        elif ((proprietary == False) and ((mechanism == '@') or (mechanism == '@Rt') or (mechanism == 'Imp@')) and explicit and (follower == False) and (ori_item_id == False) and (ori_author_id == False)):
            #print "this is Orphan@! 00101000 or 01101000"
            isRt = True
            rt_type = "Orphan@"
            
        else:
            
            print "I don't know what this is :("

    if rtn_type == 'bool':
        to_rtn = isRt
    elif rtn_type == 'split':
        to_rtn = [isRt, rt_type]
    elif rtn_type == 'three':
        to_rtn = [isRt, isWhichRt(rt_type)]
    else:
        print "rtn_type error!"
        
    return to_rtn

def isWhichRt(rt):

    vis_list = ['PRtF', 'PRtnF', 'Rt@F', 'Rt@nF', 'RtF', 'RtnF', 'P@RtF', 'P@RtnF', '@RtF', '@RtnF']
    dark_list = ['RtF_d', 'RtnF_d', 'P@F_d', 'P@nF_d', '@F_d', '@nF_d']
    orph_list = ['OrphanRt', 'Orphan@']
    # ori_list = ['ori', 'P@F', 'P@nF', '@F', '@nF']
    
    if rt in vis_list:
        to_rtn = "visible"
    elif rt in dark_list:
        to_rtn = "dark"
    elif rt in orph_list:
        to_rtn = "orphan"
    else:
        to_rtn = "ori"

    return to_rtn

def isTypeRt(row, rt):

    to_rtn = False
    
    to_check = isRt(row, 'split')

    if to_check[0] and to_check[1] == rt:
        to_rtn = True

    return to_rtn

def countRtsFile(files, rtn_type = 'split'):

    import csv

    if rtn_type == 'split':
        splits = {'ori': 0, 'P@F': 0, 'P@nF': 0, '@F': 0, '@nF': 0,
                  'PRtF': 0, 'PRtnF': 0, 'Rt@F': 0, 'Rt@nF': 0, 'RtF': 0, 'RtnF': 0, 'P@RtF': 0, 'P@RtnF': 0, '@RtF': 0, '@RtnF': 0,
                  'RtF_d': 0, 'RtnF_d': 0, 'P@F_d': 0, 'P@nF_d': 0, '@F_d': 0, '@nF_d': 0,
                  'OrphanRt': 0, 'Orphan@': 0}
    else:
        splits = dict()
        
    for filename in files:
        
        # do preliminary check to see filename's not dupes
        if os.path.isfile(filename):

            # open filename
            rfile = open(filename, 'rb')
            csv_reader = csv.reader(rfile)
            columns = csv_reader.next()
            csv_reader = csv.DictReader(rfile, columns)

            for row in csv_reader:
                rtrow = isRt(row, rtn_type)
                
                if rtn_type == 'split' or rtn_type == 'three':
                    the_key = rtrow[1]
                else:
                    the_key = rtrow
                    
                if the_key in splits:
                    splits[the_key] = splits[the_key] + 1
                else:
                    splits[the_key] = 1
                    
    return splits
