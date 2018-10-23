import sqlite3
import pandas as pd

FILE = "RC_2015-01.db"

#Declare variables to be used. We will pull only 5000 once from the db in order to maitain it not laggy.
#Also we will pull all the data ordered by time(unix). We'll create a small file so we test some basic functionalities.
connection = sqlite3.connect(FILE)
cursor = connection.cursor()
limit = 5000
last_unix = 0
currentLength = limit
pulls = 0
test = False

while limit == currentLength:
	#Pull 5000 rows from the database
	data = pd.read_sql("""SELECT * FROM parentReply WHERE unix > {} AND parent NOT NULL AND score > 0
						  ORDER BY unix ASC LIMIT {}""".format(last_unix, limit), connection)
	#Update variables
	last_unix = data.tail(1)['unix'].values[0]
	currentLength = len(data)

	if not test:
		file = "test"
		test = True
	else:
		file = "train"

	#Create files and write all data divided in 2 files : from --> to such as every line in 'from' has the reply in 'to'
	with open("{}.from".format(file), 'a', encoding='utf8') as f:
		for content in data['parent'].values:
			f.write(content + '\n')
	with open("{}.to".format(file), 'a', encoding='utf8') as f:
		for content in data['comment'].values:
			f.write(content + '\n')

	#Make it verbose
	pulls += 1
	if pulls % 20 == 0:
		print(pulls * limit, 'rows done')