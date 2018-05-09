import csv
import json
from pymongo import MongoClient
from datetime import datetime
client = MongoClient()
db = client.flex_data
DATAFILE06 = 'cpu2006-results-20180201-173831_Short.csv'
DATAFILE17 = 'cpu2017-results-20180201-173755_short.csv'
HARDWARE_FIELDS = ['Benchmark', 'Hardware Vendor', 'System', 'Operating System', 'File System']
CPU06_FIELDS = ['Result', 'Baseline', '# Cores', '# Chips ', '# Cores Per Chip ', '# Threads Per Core', 'Processor', 'Processor MHz', 'Processor Characteristics', 'CPU(s) Orderable', 'Auto Parallelization', 'Base Pointer Size', 'Peak Pointer Size', '1st Level Cache', '2nd Level Cache', '3rd Level Cache', 'Other Cache', 'Memory', 'Operating System', 'File System', 'Compiler']
CPU17_FIELDS = ['Peak Result', 'Base Result', 'Energy Peak Result', 'Energy Base Result', '# Cores', '# Chips', '# Enabled Threads Per Core', 'Processor', 'Processor MHz', 'CPU(s) Orderable', 'Parallelization', 'Base Pointer Size', 'Peak Pointer Size', '1st Level Cache', '2nd Level Cache', '3rd Level Cache', 'Other Cache', 'Memory', 'Compiler']
hw_data = []
cpu_data = []
OBJECT_ID_2006 = 400000000
OBJECT_ID_2017 = 500000000
fh06 = open(DATAFILE06, "rt", encoding = 'utf-8')
fh17 = open(DATAFILE17, "rt", encoding = 'utf-8')
reader06 = csv.DictReader(fh06)
reader17 = csv.DictReader(fh17)
next(reader06)
next(reader17)
id = OBJECT_ID_2006
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
				if field == '# Cores':
					cpu_temp['Cores'] = val
				elif field == '# Chips ':
					cpu_temp['Chips'] = val
				elif field == '# Cores Per Chip ':
					cpu_temp['Cores_Per_Chip'] = val
				elif field == '# Threads Per Core':
					cpu_temp['EnabledThreads'] = val
				elif field == 'Processor':
					cpu_temp['Processor_MHz'] = val
				elif field == 'Processor MHz':
					cpu_temp['Frequency'] = val
				elif field == 'Processor Characteristics':
					cpu_temp['Processor_Characteristics'] = val
				elif field == 'CPU(s) Orderable':
					cpu_temp['CPU_Orderable'] = val
				elif field == 'Auto Parallelization':
					cpu_temp['Auto_Parallelization'] = val
				elif field == 'Base Pointer Size':
					cpu_temp['BasePointerSize'] = val
				elif field == 'Peak Pointer Size':
					cpu_temp['PeakPointerSize'] = val
				elif field == '1st Level Cache':
					cpu_temp['FirstLevelCache'] = val
				elif field == '2nd Level Cache':
					cpu_temp['SecondLevelCache'] = val
				elif field == '3rd Level Cache':
					cpu_temp['ThirdLevelCache'] = val
				elif field == 'Other Cache':
					cpu_temp['OtherCache'] = val
				else:
					cpu_temp[field] = val
		# Only add required fields
		if field in HARDWARE_FIELDS:
			if field == 'Hardware Vendor':
				hw_temp['Hardware_Vendor'] = val
			elif field == 'Operating System':
				hw_temp['Operating_System'] = val
			elif field == 'File System':
				hw_temp['File_System'] = val
			else:
				hw_temp[field] = val
	# Append users to our data list
	hw_data.append(hw_temp)
	cpu_data.append(cpu_temp)
fh06.close()
id = OBJECT_ID_2017
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
				if field == 'Peak Result':
					cpu_temp['PeakResult'] = val
				elif field == 'Base Result':
					cpu_temp['BaseResult'] = val
				elif field == 'Energy Peak Result':
					cpu_temp['EnergyPeakResult'] = val
				elif field == 'Energy Base Result':
					cpu_temp['EnergyBaseResult'] = val
				elif field == '# Cores':
					cpu_temp['Number_Cores'] = val
				elif field == '# Chips':
					cpu_temp['Chips'] = val
				elif field == '# Enabled Threads Per Core':
					cpu_temp['EnabledThreads'] = val
				elif field == 'Processor':
					cpu_temp['Processor_MHz'] = val
				elif field == 'Processor MHz':
					cpu_temp['Frequency'] = val
				elif field == 'CPU(s) Orderable':
					cpu_temp['CPU_Orderable'] = val
				elif field == 'Base Pointer Size':
					cpu_temp['BasePointerSize'] = val
				elif field == 'Peak Pointer Size':
					cpu_temp['PeakPointerSize'] = val
				elif field == '1st Level Cache':
					cpu_temp['FirstLevelCache'] = val
				elif field == '2nd Level Cache':
					cpu_temp['SecondLevelCache'] = val
				elif field == '3rd Level Cache':
					cpu_temp['ThirdLevelCache'] = val
				elif field == 'Other Cache':
					cpu_temp['OtherCache'] = val
				else:
					cpu_temp[field] = val
		# Only add required fields
		if field in HARDWARE_FIELDS:
			if field == 'Hardware Vendor':
				hw_temp['Hardware_Vendor'] = val
			elif field == 'Operating System':
				hw_temp['Operating_System'] = val
			elif field == 'File System':
				hw_temp['File_System'] = val
			else:
				hw_temp[field] = val
	# Append users to our data list
	hw_data.append(hw_temp)
	cpu_data.append(cpu_temp)
fh17.close()
with open('hardware.json', 'w') as outfile:
	json.dump(hw_data, outfile)
with open('cpu.json', 'w') as outfile:
	json.dump(cpu_data, outfile)	
with open('hardware.json') as dat:
	d = json.loads(dat.read())
	db.hardware_information.insert(d)
print ("Success")	
with open('cpu.json') as dat:
	d = json.loads(dat.read())
	db.cpu_information.insert(d)
print ("Success")
