# -*- coding: utf-8 -*-

import pandas as pd
import boto3
import io

geoj =''

_BUCKET_NAME = 'kt-client'

files = []
client = boto3.client('s3',
	     aws_access_key_id = '',
	     aws_secret_access_key = '',)
paginator = client.get_paginator('list_objects')
page_iterator = paginator.paginate(Bucket = 'kt-climate')
for page in page_iterator:
	files.append(page['Contents'])
#response = client.list_objects()
#for content in response.get('Content', []):#
#	files.append(content.get('Key'))

for page in files:
    for content in page:
        filename = content.get('Key')
        print(filename)
        obj = client.get_object(Bucket= 'kt-climate', Key = filename)
        df = pd.read_fwf(io.BytesIO(obj['Body'].read()))
        month = []
        i = 0
        while i < len(df):
            x = df['YEARMODA'][i]
            x = str(x)
            mo = x[4:6]
            month.append(mo)
            i += 1
        df['month'] = pd.Series(month)
        year = str(df['YEARMODA'][0])[0:4]
        montemp = df.groupby('month')['TEMP'].mean()
        cTemp = []
        for temp in montemp:
	        ct = (temp - 32) * 5/9
	        ct = round(ct, 2)
	        cTemp.append(ct)

        dfstat = pd.read_fwf('station_list.csv')
        station = df['STN---'][0]
        station = '0'+str(station)
        print(station)

        i = 0
        while i < len(dfstat):
	        if station == dfstat['NUMBER'][i]:
		        lat = dfstat['LAT'][i]
		        lon = dfstat['LON'][i]
	        i += 1
        print(lat, lon)

        latit = lat[0:2]+'.'+lat[2:4]
        if lat[-1:] == 'S':
	        latit = '-'+latit
        longit = lon[0:3]+'.'+lon[3:5]
        if lon[-1:] == 'W':
	        longit = '-'+longit

        # then have to write that lot out to geojson
        # latit, longit, year, month, avtemp

        i = 0

        while i < len(cTemp):
            geoj = geoj + '{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [' + latit +', '+ longit + ']},"properties": {"year": '+year+'"month":'+month[i]+'"avtemp": '+str(cTemp[i]) +'}}'
            i += 1

#

