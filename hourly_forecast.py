import forecastio
import json
from darksky import forecast
import pymssql
import datetime
from datetime import timedelta
import pymongo
from pymongo import MongoClient

key = "7f63097044b5ed90944f29481d002ea8"

#client = pymongo.MongoClient("mongodb+srv://gfitzy123:r290mE9FFSKeazKG@cluster0-mr9wf.mongodb.net/test?retryWrites=true")
#client = pymongo.MongoClient("mongodb://gfitzy123:r290mE9FFSKeazKG@cluster0-shard-00-00-mr9wf.mongodb.net:27017,cluster0-shard-00-01-mr9wf.mongodb.net:27017,cluster0-shard-00-02-mr9wf.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true")

client = pymongo.MongoClient("mongodb://gfitzy123:r290mE9FFSKeazKG@cluster0-shard-00-00-mr9wf.mongodb.net:27017,cluster0-shard-00-01-mr9wf.mongodb.net:27017,cluster0-shard-00-02-mr9wf.mongodb.net:27017/test?replicaSet=Cluster0-shard-0&retryWrites=true")
db = client['forecasts']
print(db)
collection = db['hourly_forecasts']

print('f', pymongo.version)

miso_states = {}
miso_states['Iowa'] = None
# miso_states['Indiana'] = None
# miso_states['Illinois'] = None
# miso_states['Wisconsin'] = None
# miso_states['Michigan'] = None
# miso_states['North Dakota'] = None
# miso_states['Texas'] = None
# miso_states['Louisiana'] = None
# miso_states['Arkansas'] = None
# miso_states['Missouri'] = None

# server = "JPTCSQL4\\JPTCSQL4"
# user = "sa"
# password = "workhard"

# conn = pymssql.connect(server, user, password, "commondata")
# cursor = conn.cursor()

with open('C:\scripts\Weather\dark_sky\cities.json') as json_data:
    cities_list = json.load(json_data)
    # print(citie)s


for obj in cities_list:
	state_name = obj['state']
	exists = miso_states.get(state_name, 'not_found')

	if exists != 'not_found':
		lat = obj['latitude']
		lng = obj['longitude']

		forecast_result = forecast(key, lat, lng)

		for index, item in enumerate(forecast_result['hourly']['data']):

			r = forecast_result['hourly']['data'][index]
			
			r['stateName'] = state_name
			r['city'] = obj['city']
			r['latitude'] = obj['latitude']
			r['longitude'] = obj['longitude']
			r['population'] = obj['population']
			r['rank'] = obj['rank']
			
			# {'apparentTemperature': 72.72, 
			# 'visibility': 10, 
			# 'time': 1528394400, 
			# 'ozone': 316.93, 
			# 'temperature': 72.72, 
			# 'cloudCover': 0.24, ttpdwpP
			# 'uvIndex': 9,wdP
			# 'icon': 'clear-day',
			# 'pressure': 1016.03, w
			# 'windBearing': 52,
			# 'precipIntensity': 0,
			# 'humidity': 0.59,
			# 'dewPoint': 57.43, 
			# 'windGust': 7.05,
			# 'windSpeed': 3.75, 
			# 'summary': 'Clear', 
			# 'precipProbability': 0}
			r['date'] = datetime.datetime.fromtimestamp(r['time']).strftime('%Y-%m-%d')
			r['ts'] = datetime.datetime.fromtimestamp(r['time']).strftime('%Y-%m-%d %H:%M:%S')

			add_hour_to_he = datetime.datetime.fromtimestamp(r['time']) + timedelta(hours=1)
			he = '{:%H}'.format(add_hour_to_he)
			r['he'] = he
			epoch_time = r['time']

			print(r)

			collection.insert_one(r)

			# cursor.execute("""
			# INSERT INTO weather_underground_test (
			# ts,
			# he,
			# city, 
			# stateName, 
			# latitude, 
			# longitude, 
			# temperature, 
			# apparentTemperature, 
			# precipProbability, 	
			# precipIntensity,
			# windBearing, 	
			# windSpeed, 		
			# windGust, 	
			# cloudCover, 				
			# visibility,
			# ozone,
			# uvIndex, 
			# icon, 
			# pressure, 
			# humidity, 
			# dewPoint, 
			# summary, 
			# population, 
			# rank, 
			# date, 
			# time)
			# VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
			# """, (
			# 	r['ts'],
			# 	r['he'],				
			# 	r['city'],
			# 	r['stateName'],				
			# 	r['latitude'],
			# 	r['longitude'],
			# 	r['temperature'], 				
			# 	r['apparentTemperature'], 
			# 	r['precipProbability'],
			# 	r['precipIntensity'],
			# 	r['windBearing'], 
			# 	r['windSpeed'], 
			# 	r['windGust'], 
			# 	r['cloudCover'], 
			# 	r['visibility'], 
			# 	r['ozone'],
			# 	r['uvIndex'], 
			# 	r['icon'], 
			# 	r['pressure'], 
			# 	r['humidity'],
			# 	r['dewPoint'], 
			# 	r['summary'], 
			# 	r['population'],
			# 	r['rank'],
			# 	r['date'],
			# 	r['time'], 				
			# 	))






client.close()
# you must call commit() to persist your data if you don't set autocommit to True
exit();


