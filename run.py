import sys, os
sys.path.append(os.path.join(os.getcwd(), "site-packages"))

import datetime
import time
import csv
from collections import defaultdict
import glob
import numpy as np
import pandas as pd
import redis

'''
loading cluster files of all six years
'''
all_files = defaultdict()
cluster_files = glob.glob("./Clusters_method2_merged/*");
for file in cluster_files:
	all_files[file[9:]] = os.listdir(file)
'''
function to find cluster of predicted date
'''

'''
Establishing a connection with the Redis cache over variable 'r'
Hostname: testidday.redis.cache.windows.net
Port: 6380
Pwd: dVNtF8HC94HBv4pTnDqJGVwmWUlRz0APMU+Gp1W+dOQ=
SSL: True
'''
try:
	r = redis.StrictRedis(host='testidday.redis.cache.windows.net', port=6380, password='dVNtF8HC94HBv4pTnDqJGVwmWUlRz0APMU+Gp1W+dOQ=',ssl=True)
	#r = redis.StrictRedis(host='localhost', port=6379)
	test_response = r.client_list()
except redis.ConnectionError:
	print("Error: \tCould not connect to database. \n\tCheck Credentials.")
	sys.exit()

	hash_map = {}
	hash_map["Date"] = datetime.date.today().strftime("%B %d, %Y")
	r.set("MLRecommendations", "Starts at 09:31")


'''
Dynamically calculates 5 point moving average for incoming live feed
'''
def running_mean(l, N):
	result=list()
	for i in range(N-1,len(l)):
		result.append(sum(l[i-N+1:i+1]) / N)
	return result

'''
matplot's 'ggplot' style of graphs used.
'''	
#matplotlib.style.use('ggplot')

'''
Loading 5 years of data, from 2012 to 2016.
'''
files = glob.glob("./MA_5_930_1.5_chosen_one/ALL_DATA_NEW_5/NIFTY*.csv")
files.sort()

save=dict() #Saves the correlation in an hourly basis
hash_map=dict() #Used to update the hashmap on the Redis Cache with the market's predicted behaviour for every 3 minutes
all = list() #Stores all the dataframes which has been loaded


'''
This for loop is used to sanitize the input dataframe, extracting only the columns we need and filtering datapoints such that
it starts from 09:30 am and goes on till 03:25 pm
'''
for file in files:
	#Read the file
	df = pd.read_csv(file,header=None,names = ["NIFTY","Date", "Time", "Open", "High","Low","Close","UNS","k"]);
	#Get only the required columns
	df = df.ix[:,1:7]
	x=0
	while(x+354 < len(df) ):
		while(df['Time'][x] != "09:31"):
			x+=1
			if(df['Time'][x] == "09:31"):
				if(df['Time'][x+354] == "15:25"):
					all.append(df.ix[x:x+354,:].reset_index()) #Reset indices used to make sure that 'all' has one uniform index throughout
		x+=354

'''
Start Processing live data.
Rescans for new datapoints every fixed interval (10 seconds)
'''
try:
	while(True):
		
		#live_feed_string = r.hgetall("nw##default")[1]
		#print(live_feed_string)

		with open("scraped.csv","w",newline="") as csvfile:
			fieldnames=['NIFTY','Date','Time','Open','High','Low','Close']
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
			writer.writeheader()
			try:
				for KV in r.hscan_iter("nw##SMA(5)"):
					value = KV[1].decode('ascii')
					d = value.split(',')
					writer.writerow({'NIFTY': d[0],'Date': d[1],'Time': d[2], 'Open': d[3],'High': d[4], 'Low': d[5],'Close': d[6]})
			except:
				print("Error: \tData inconsistent \n\tRe-fetching in 20 seconds")
				time.sleep(20)
				continue

		df_live = pd.read_csv("scraped.csv")

		y=0
		
		while(df_live['Time'][y] != "09:31"):
			y+=1
		if(df_live['Time'][y] == "09:31"):
			df_live=(df_live.ix[y: ,:].reset_index())
			df_live=df_live.ix[:,1:]
		print("\tWaiting for Data...")
		print(df_live)
		
		'''
		Write the currently populated live feed dataframe that is smoothed by 5MA into "todays_date.csv"
		Note: "scraped.csv" has raw data.
			  "todays_date.csv" has smoothened data
		'''
		#df_live.to_csv(str(df_live['Date'][0])+".csv")
		
		
		test=pd.DataFrame()
		corr=list()
		'''
		Checks if live feed is at a three minute interval.
		If so:
			Runs through all days in 'all'
		'''
		if(len(df_live)%3==0):
			test=df_live
			timing=str(df_live['Time'][len(df_live)-1]) #Stores the time at which we found a 3 minute interval
			for i in range(0,len(all)):
				#corr.append(pearsonr(list(all[i]['Close'][0:3*int((len(df_live)/3))]),list(test.Close))[0])
				corr.append(np.corrcoef(list(all[i]['Close'][0:3*int((len(df_live)/3))]), list(test.Close))[0,1])
			
			m = max(corr) #'m' has the max correlation value
			print("Correlation Value: "+str(max(corr)))
			minutes=list(range(355)) #Makes a list of the minutes from 09:31am to 3:25pm
			index = corr.index(m) #Finds the index of the max correlated day
			print("Best matched Date: "+ str(all[index].Date[0]))

			'''
			Takes an hourly bucket to visually compare the prediction accuracy for different parts of the day 
			'''
			#if(timing=="10:00" or timing=="11:00" or timing=="12:00" or timing=="13:00" or timing=="14:00" or timing=="15:00"):
				#save[timing]=all[index].Date[0]
			r.set("MLRecommendations", str(all[index].Date[0])+'#'+str(timing)+'#'+str(m))
			r.hmset("MLRecommendationsHistory", {timing : str(all[index].Date[0])+'#'+str(timing)+'#'+str(m)})
			#r.append('MLRecommendationsHistory', hash_map[timing])
		time.sleep(20)
except:
	print("\tEnd of Live Data")