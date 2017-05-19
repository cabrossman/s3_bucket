#import os
#path = 'C:/Users/christopher.brossman/Documents/Projects/work/s3_bucket/'
#os.chdir(path)
#f = 'biBucketInfo.py'
#execfile(f)
from ftplib import FTP
import requests
import json
import pandas as pd
import boto3
import re

class biBucket(object):
    'common class for all APIs'

    
    def __init__(self, ACCESS_KEY = 'AKIAJOAPCF34GTBIUTAQ', SECRET_KEY = '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', BUCKET_ID = 'bizintel-clickstream', 
	HOST = 'ftp.omniture.com', USER = 'dominion_ftp', PASS = 'nrwLNQ4t'):
        
        self.client = boto3.client(
	's3',
	aws_access_key_id=ACCESS_KEY,
	aws_secret_access_key=SECRET_KEY,
	)
	self.bucket = BUCKET_ID
	self.ftp = FTP(HOST, USER, PASS)

            
#########################################################################################
#       Get Items in Bucket
#########################################################################################
    def listItemsInBucket(self):
	'prints items in BI bucket'
	lastIndex = 0
	items = self.client.list_objects(Bucket = self.bucket)
	#items = client.list_objects(Bucket = BUCKET_ID)
	df_index = [index + lastIndex for index in range(0,len(items['Contents']))]
	df_columns = [u'ETag', u'Key', u'LastModified', u'Owner', u'Size', u'StorageClass']
	df = pd.DataFrame(items['Contents'], index = df_index, columns = df_columns)
	lastIndex = df.index[-1]
	df_list = [df]
	n = 1
	#Amazon limits results to 1000. To get more than 1000 we need to loop through
	while len(items['Contents']) == 1000:
		print('iterations: ' + str(n))
		lastKey = items['Contents'][999]['Key']
		items = self.client.list_objects(Bucket = self.bucket, Marker = lastKey)
		#items = client.list_objects(Bucket = BUCKET_ID, Marker = lastKey)
		df_index = [index + lastIndex for index in range(1,len(items['Contents']) + 1)]
		df = pd.DataFrame(items['Contents'], index = df_index, columns = df_columns)
		lastIndex = df.index[-1]
		print('lastIndex: ' + str(lastIndex))
		df_list.append(df)
		n = n + 1
	
	df_all = pd.concat(df_list)
	return df_all


if __name__ == "__main__":
	X = biBucket()
	df = X.listItemsInBucket()
	files = df['Key'].tolist()
	def getMatch(fileName):
		if re.search("01-demidas_([0-9]{8})-", fileName) is None:
			return None
		else:
			return re.search("01-demidas_([0-9]{8})-", fileName).groups()[0]
	#filesDates = list(set([re.search("01-demidas_([0-9]{8})-", zfile).groups()[0] for zfile in files]))
	filesDates = list(set([getMatch(zfile) for zfile in files]))
	filesDates.sort()
	for i in filesDates:
		print i
	
	print 'total number of days ' + str(len(filesDates) -1)
	
#listOfItemsInBucket = client.list_objects(Bucket = 'bizintel-clickstream')
#files = [file['Key'] for file in listOfItemsInBucket['Contents']]
