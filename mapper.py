#!/usr/bin/env python3

import json
import time
import sys



final_list = []
for i in sys.stdin:
    condition = 0
    data = json.loads(i)
    
    if("lane blocked" in (str(data['Description']).lower()) or "shoulder blocked" in (str(data['Description']).lower()) or "overturned vehicle" in (str(data['Description']).lower())):
    	condition+=1
    if((data['Severity'])>=2):
    	condition+=1
    if(str(data['Sunrise_Sunset']).lower()=="night"):
    	condition+=1
    if(data['Visibility(mi)']<=10):
    	condition+=1
    if(data['Precipitation(in)']>=0.2):
    	condition+=1
    if(str(data['Weather_Condition']).lower()=="heavy snow" or str(data['Weather_Condition']).lower()=="thunderstorm" or  str(data['Weather_Condition']).lower()=="heavy rain" or str(data['Weather_Condition']).lower()=="heavy rain showers" or str(data['Weather_Condition']).lower()=="blowing dust"):
    	condition+=1
    if(condition==6):
    	final_list.append(data)

for record in final_list:
   start_hour = int(record['Start_Time'].split()[1].split(':')[0])

   if(start_hour<10):
   	start_hour = '0' + str(start_hour)
   print(start_hour,"\t",1)

