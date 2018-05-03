import csv
import json
from pymongo import MongoClient
from datetime import datetime
client = MongoClient()
db = client.users

DATAFILE = 'WW5 LaaS weekly usage reports.csv'
FIELDS = ['log_sid', 'User_Name', 'Address', 'Client_Host', 'Client_IP', 'Virtual_IP', 'resource_var']
userDict = {}
data = []
usersFound = []
fh = open(DATAFILE, "rt", encoding = 'utf-8')
reader = csv.reader(fh)
# This first section goes through and makes adjustments, 
# such as converting the date from string to datetime 
# and removing the <br/> found in some of the usernames
#it also appends the login dates and SID's to a username that
#that has already been found at least once already so that duplicate users
#are not added into the database
for i, rows in enumerate(reader):
	if i == 0: continue
	# if there is a username for this row
	if not rows[11] in (None, ""):
		origName = str(rows[11])
		if "<br/>" in origName:
			#remove <br/> from user names
			newName = origName.split("<br/>")	
			rows[11] = newName[1]
		# u = username
		u = rows[11]
		# k = log_sid
		k = rows[0]
		bo = rows[6]
		bi = rows[5]
		co = rows[9]
		st = rows[10] 
		# if there is a login date for this row then convert the format to datetime
		if not rows[2] in (None, ""):
			t = datetime.strptime(rows[2], "%m/%d/%Y %H:%M")
			v = t.strftime('%m/%d/%Y %H:%M')
		else:
			# do not convert the date because there is no date for this row
			v = rows[2]
		if not u in userDict:
			# if this username is not already a key in the userDict then create a key with a dictionary for the value
			userDict[u] = [{'SID' : k, 'Date' : v, 'Bytes_In' : bi, 'Bytes_Out' : bo, 'Country' : co, 'State' : st}]
		else:
			# if there already is a key corresponding with this username then append this dictionary as a value
			userDict[u].append({'SID' : k, 'Date' : v, 'Bytes_In' : bi, 'Bytes_Out' : bo, 'Country' : co, 'State' : st})
	# if there is not a username for this row
	else:
		# k = log_sid
		k = rows[0]
		bo = rows[6]
		bi = rows[5]
		co = rows[9]
		st = rows[10]
		# if there is a a login date
		if not rows[2] in (None, ""):
			t = datetime.strptime(rows[2], "%m/%d/%Y %H:%M")
			v = t.strftime('%m/%d/%Y %H:%M')
		else:
			v = rows[2]
		if not k in userDict:
			userDict[k] = [{'SID' : k, 'Date' : v, 'Bytes_In' : bi, 'Bytes_Out' : bo, 'Country' : co, 'State' : st}]
		else:
			userDict[k].append({'SID' : k, 'Date' : v, 'Bytes_In' : bi, 'Bytes_Out' : bo, 'Country' : co, 'State' : st})

fh.close()
fh = open(DATAFILE, "rt", encoding = 'utf-8')

reader = csv.DictReader(fh)
next(reader)
foundUsers = []

# This second part iterates through each line the csv file and adds the user name to a temporary dictionary
# and then appends that dictionary to a list called data. data is a list of dictionaries which will be converted to 
# JSON later
# Iterate through each user...
for line in reader:
	temp = {}
	for f, v in line.items():
		if (f == 'User_Name'):
			if (v != ""):
				origName = str(v)
				if "<br/>" in origName:
					#remove <br/> from user names
					newName = origName.split("<br/>")	
					v = newName[1]
				if v in foundUsers:
					continue
				# Iterate through each field of target user
				foundUsers.append(v)
				for field, val in line.items():
					if field not in FIELDS:
						continue
					# Only add required fields
					if field in FIELDS:
						if field != 'User_Name':
							if field != 'log_sid':
								temp[field] = val
						elif val in userDict:
							t = datetime.strptime("4/10/2018 8:15", "%m/%d/%Y %H:%M")
							u = t.strftime('%m/%d/%Y %H:%M')
							userDict[v].append({'SID' : 00000, 'Date' : u, 'Bytes_In' : 123456, 'Bytes_Out' : 54321, 'Country' : "US", 'State' : "California"})
							t = datetime.strptime("4/14/2018 9:56", "%m/%d/%Y %H:%M")
							u = t.strftime('%m/%d/%Y %H:%M')
							userDict[v].append({'SID' : 11111, 'Date' : u, 'Bytes_In' : 654321, 'Bytes_Out' : 12345, 'Country' : "NL", 'State' : "Noord-Holland"})
							t = datetime.strptime("4/8/2018 10:15", "%m/%d/%Y %H:%M")
							u = t.strftime('%m/%d/%Y %H:%M')
							userDict[v].append({'SID' : 22222, 'Date' : u, 'Bytes_In' : 123456, 'Bytes_Out' : 54321, 'Country' : "US", 'State' : "Michigan"})
							t = datetime.strptime("4/1/2018 7:15", "%m/%d/%Y %H:%M")
							u = t.strftime('%m/%d/%Y %H:%M')
							userDict[v].append({'SID' : 33333, 'Date' : u, 'Bytes_In' : 123456, 'Bytes_Out' : 54321, 'Country' : "CN", 'State' : "Zhejiang"})
							t = datetime.strptime("3/17/2018 5:15", "%m/%d/%Y %H:%M")
							u = t.strftime('%m/%d/%Y %H:%M')
							userDict[v].append({'SID' : 44444, 'Date' : u, 'Bytes_In' : 123456, 'Bytes_Out' : 54321, 'Country' : "JP", 'State' : "Tokyo"})
							temp['Login_Activity'] = userDict[v]
							temp[field] = v
			else:
				for field, val in line.items():
					if field not in FIELDS:
						continue
					# Only add required fields
					if field in FIELDS:
						if field != 'log_sid':
							temp[field] = val
						elif val in userDict:
							temp['Login_Activity'] = userDict[val]
	# Append users to our data list
	data.append(temp)
fh.close()
with open('users.json', 'w') as outfile:
	json.dump(data, outfile)
#This part is for dumping the json into a mongo database
with open('users.json') as dat:
	d = json.loads(dat.read())
	db.user_information.insert(d)