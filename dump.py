import settings
import tweepy
import dataset
from datafreeze import freeze
from textblob import TextBlob

TABLE_NAME = "elite" #settings.TABLE_NAME
CONNECTION_STRING = "sqlite:///elite.db"
OUTPUT_NAME = "elite"

db = dataset.connect(CONNECTION_STRING)

#result = db[TABLE_NAME].all()
#freeze(result, format='csv', filename=OUTPUT_NAME + ".csv")
#result = db.query('SELECT * FROM ' + TABLE_NAME + ' WHERE id > 34')
#freeze(result, format='json', indent=4, wrap=False, filename=OUTPUT_NAME + ".json")