#!/usr/bin/env python3

import sys
hour = None
current_hour = None
current_count = 0


for line in sys.stdin:
    line = line.strip()
    hour, count = line.split('\t',1)

    try:
        count = int(count)
        hour=int(hour)
    except ValueError:
        continue

    if current_hour == hour:
        current_count += count
    
    else:
        if (current_hour!=None):
            print(current_hour,"\t",current_count)
        current_count = count
        current_hour = hour

if current_hour == hour:
    print(current_hour,"\t",current_count)
