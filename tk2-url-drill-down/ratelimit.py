import time

def getKeys(limit, f = 'search', key = 'prime'):
    
    if key == 'prime':
        time.sleep(1)
        if enoughLimit(limit, f, key):
            return key
        elif enoughLimit(limit, f, 'alt1'):
            print "changing to alt1 keys"
            return 'alt1'
        elif enoughLimit(limit, f, 'alt2'):
            print "changing to alt2 keys"
            return 'alt2'
        elif enoughLimit(limit, f, 'alt3'):
            print "changing to alt3 keys"
            return 'alt3'
        else:
            # choose key w least waiting time
            return waitKey(f, key)
        
def getKeyData(key = 'prime', f = 'search'):

    time.sleep(1)
    
    # username 1
    if key == 'prime':
        return 'consumer_key0', 'consumer_secret0', 'key0', 'secret0'
    
    # username 2
    elif key == 'alt1':
        return 'consumer_key1', 'consumer_secret1', 'key1', 'secret1'
    
    # username 3
    elif key == 'alt2':
        return 'consumer_key2', 'consumer_secret2', 'key2', 'secret2'
        
    # username 4
    elif key == 'alt3':
        return 'consumer_key3', 'consumer_secret3', 'key3', 'secret3'
        
def getLimit(f = 'search', key = 'prime'):
    return commonRateLimit('get_limit', f, key)

def enoughLimit(limit, f = 'search', key = 'prime'):

    if limit < getLimit(f, key):
        return True
    else:
        return False

def wait(f = 'search', key = 'prime'):
    return commonRateLimit('wait', f, key)

def waitKey(f = 'search', key = 'prime'):
    
    # get key w least waiting time
    wait_times = {key: wait(f, key), 'alt1': wait(f, 'alt1'), 'alt2': wait(f, 'alt2'), 'alt3': wait(f, 'alt3')}
    for key, value in sorted(wait_times.iteritems(), key=lambda (k,v): (v,k)):
        waiting_time = value
        return_key = key
        break
    
    print "rate limited at %s! need to wait %d s" % (time.strftime("%H:%M:%S"), waiting_time)
    print "current limit: %s" % getLimit(f, return_key)
    time.sleep(waiting_time)
    print "woken up at %s!" % time.strftime("%H:%M:%S")
    return return_key

def commonRateLimit(script, f = 'search', key = 'prime'):
    
    import oauth_req
    import simplejson
    import sys
    loop = True
    to_return = 0

    while loop:

        try:
            if f == 'timeline':
                url = 'https://api.twitter.com/1.1/application/rate_limit_status.json?resources=statuses'
            else:
                url = 'https://api.twitter.com/1.1/application/rate_limit_status.json?resources=%s' % f

            consumer_key, consumer_secret, kunci, rahsia = getKeyData(key, f)

            api_response = oauth_req.OauthReq(url, consumer_key, consumer_secret, kunci, rahsia)

            if api_response[0]['status'] == '200' and api_response[1]:

                json_response = simplejson.loads(api_response[1])

                if f == 'search':
                    resource = json_response['resources'][f]['/%s/tweets' % f]
                elif f == 'users' or f == 'statuses':
                    resource = json_response['resources'][f]['/%s/show/:id' % f]
                elif f == 'friendships':
                    resource = json_response['resources'][f]['/%s/show' % f]
                elif f == 'friends' or f == 'followers':
                    resource = json_response['resources'][f]['/%s/ids' % f]
                elif f == 'timeline':
                    resource = json_response['resources']['statuses']['/statuses/user_%s' % f]

                if script == 'get_limit':
                    to_return = int(resource['remaining'])
                    loop = False

                elif script == 'wait':
                    wait_time = resource['reset'] - int(time.time())
                    to_return = int(wait_time)
                    loop = False
                    
            elif (api_response[0]['status'] == '420') or (api_response[0]['status'] == '429'):
                if script == 'get_limit':
                    to_return = 150
                    loop = False
                elif script == 'wait':
                    to_return = 0
                    loop = False
                else:
                    print "Error %s: be patient my child.. %s" % (api_response[0]['status'], time.strftime("%H:%M:%S"))
                    time.sleep(20)
                
            else:
                print "Error status %s for script %s, trying again.. %s" % (api_response[0]['status'], script, time.strftime("%H:%M:%S"))
                time.sleep(1)
            
        except:
            print "%s unexpected error: %s, trying again.. %s" % (script, sys.exc_info()[0], time.strftime("%H:%M:%S"))

    return to_return

def checker(secs, f = 'search'):
    
    if enoughLimit(secs, f) == False:
        waiting_t = wait(f)
        print "rate limited at %s! neet to wait %d s" % (time.strftime("%H:%M:%S"), waiting_t)
        print "current limit: %s" % getLimit(f)
        time.sleep(waiting_t)
        print "woken up at %s!" % time.strftime("%H:%M:%S")
