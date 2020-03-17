import settings
import tweepy
import dataset
from textblob import TextBlob
from sqlalchemy.exc import ProgrammingError
import json

db = dataset.connect(settings.CONNECTION_STRING)

class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        if status.retweeted:
            return
   
        created_at = status.created_at
        id_str = status.id_str
        text = status.text
        
        in_reply_to = status.in_reply_to_screen_name

        user_name = status.user.screen_name
        user_location = status.user.location
        user_description = status.user.description
        user_created_at = status.user.created_at
        followers_count = status.user.followers_count
        friends_count = status.user.friends_count
        
        coords = None
        geo = None
        
        retweets_count = status.retweet_count
        favorites_count = status.favorite_count
        
        blob = TextBlob(text)
        sent = blob.sentiment
        
#         longitude = None
#         latitude = None
#         if status.coordinates:
#             longitude = status.coordinates['coordinates'][0]
#             latitude = status.coordinates['coordinates'][1]

        if status.geo:
            geo = json.dumps(geo)

        if status.coordinates:
            coords = json.dumps(coords)

        table = db[settings.TABLE_NAME]
        try:
            table.insert(dict(            
                created_at=created_at,
                id_str=id_str,
                text=text,
                in_reply_to = in_reply_to,
                
                user_name=user_name,
                user_location=user_location,
                user_description=user_description,
                user_created=user_created_at,
                geo=geo,
                coordinates=coords,
                user_followers_count=followers_count,
                user_friends_count=friends_count,
                
                retweet_count=retweets_count,
                favorites_count=favorites_count,
                
                polarity=sent.polarity,
                subjectivity=sent.subjectivity,
            ))
        except ProgrammingError as err:
            print(err)

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

#Remove links and special characters        
def clean_tweet(self, tweet):     
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) \
                                |(\w+:\/\/\S+)", " ", tweet).split()) 

#Strip all non-ASCII characters
def clean_ascii(text): 
    if text:
        return text.encode('ascii', 'ignore').decode('ascii')
    else:
        return None
    
auth = tweepy.OAuthHandler(settings.TWITTER_APP_KEY, settings.TWITTER_APP_SECRET)
auth.set_access_token(settings.TWITTER_KEY, settings.TWITTER_SECRET)
api = tweepy.API(auth)

stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=settings.TRACK_TERMS)