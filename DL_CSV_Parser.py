import csv
import json
from pymongo import MongoClient
from datetime import datetime
client = MongoClient()
db = client.benchmark_data
START_ID_06 = 400000000000
START_ID_17 = 500000000000
DATAFILE06 = 'cpu2006-results-20180201-173831_Short.csv'
DATAFILE17 = 'cpu2017-results-20180201-173755_short.csv'
HARDWARE_FIELDS = ['Benchmark', 'Hardware Vendor', 'System', 'Operating System', 'File System']
CPU06_FIELDS = ['Result', 'Baseline', '# Cores', '# Chips ', '# Cores Per Chip ', '# Threads Per Core', 'Processor', 'Processor MHz', 'Processor Characteristics', 'CPU(s) Orderable', 'Auto Parallelization', 'Base Pointer Size', 'Peak Pointer Size', '1st Level Cache', '2nd Level Cache', '3rd Level Cache', 'Other Cache', 'Memory', 'Operating System', 'File System', 'Compiler']
CPU17_FIELDS = ['Peak Result', 'Base Result', 'Energy Peak Result', 'Energy Base Result', '# Cores', '# Chips', '# Enabled Threads Per Core', 'Processor', 'Processor MHz', 'CPU(s) Orderable', 'Parallelization', 'Base Pointer Size', 'Peak Pointer Size', '1st Level Cache', '2nd Level Cache', '3rd Level Cache', 'Other Cache', 'Memory', 'Compiler']
userDict = {}
hw_data = []
cpu_data = []
usersFound = []
fh06 = open(DATAFILE06, "rt", encoding = 'utf-8')
fh17 = open(DATAFILE17, "rt", encoding = 'utf-8')
reader06 = csv.DictReader(fh06)
reader17 = csv.DictReader(fh17)
next(reader06)
next(reader17)
foundUsers = []
id = START_ID_06
# Iterate through each 2006 benchmark score...
for line in reader06:
	cpu_temp = {}
	hw_temp = {}
	cpu_temp['_id'] = id
	hw_temp['_id'] = id
	id = id + 1
	for field, val in line.items():
		if field not in HARDWARE_FIELDS:
			if field in CPU06_FIELDS:
				cpu_temp[field] = val
		# Only add required fields
		if field in HARDWARE_FIELDS:
			hw_temp[field] = val
	# Append users to our data list
	hw_data.append(hw_temp)
	cpu_data.append(cpu_temp)
fh06.close()
print ("Success")
id = START_ID_17
# Iterate through each 2006 benchmark score...
for line in reader17:
	cpu_temp = {}
	hw_temp = {}
	cpu_temp['_id'] = id
	hw_temp['_id'] = id
	id = id + 1
	for field, val in line.items():
		if field not in HARDWARE_FIELDS:
			if field in CPU17_FIELDS:
				cpu_temp[field] = val
		# Only add required fields
		if field in HARDWARE_FIELDS:
			hw_temp[field] = val
	# Append users to our data list
	hw_data.append(hw_temp)
	cpu_data.append(cpu_temp)
fh17.close()
print ("Success")
with open('hardware.json', 'w') as outfile:
	json.dump(hw_data, outfile)
	
with open('cpu.json', 'w') as outfile:
	json.dump(cpu_data, outfile)
print ("Success")	
with open('hardware.json') as dat:
	d = json.loads(dat.read())
	db.hardware_information.insert(d)
print ("Success")	
with open('cpu.json') as dat:
	d = json.loads(dat.read())
	db.cpu_information.insert(d)
print ("Success")