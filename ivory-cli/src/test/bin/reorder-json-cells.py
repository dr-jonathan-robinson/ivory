#!/usr/bin/python

import sys
import fileinput
import json

delim="|"

def fix(x):
   try:
     obj = json.loads(x)
     if type(obj) == list:
       obj = sorted(obj)
     return json.dumps(obj, sort_keys=True)
   except ValueError:
     return x


for line in fileinput.input():
  print delim.join(map(fix, line.rstrip('\n').split(delim)))
