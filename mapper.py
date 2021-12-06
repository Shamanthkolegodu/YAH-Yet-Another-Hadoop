#!/usr/bin/env python3

import json
import time
import sys
for line in sys.stdin: 
    # remove leading and trailing whitespace 
    line = line.strip() 
    # split the line into words 
    words = line.split() 
    # increase counters 
    for word in words: 
        print ('%s\t%s' % (word, 1))
