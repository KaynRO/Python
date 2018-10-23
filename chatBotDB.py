import sqlite3
import json
from datetime import datetime
import sys

#Choose the apropriate encoding
reload(sys)
sys.setdefaultencoding('UTF-8')

#Get all data as a vector and then insert them all
sqlTransaction = []
FILE = "RC_2015-01"

connection = sqlite3.connect('{}.db'.format(FILE))
cursor = connection.cursor()

#Create a SQL table where we insert data
def createTable():
	cursor.execute("""CREATE TABLE IF NOT EXISTS parentReply
				  (parentID TEXT PRIMARY KEY, commentID TEXT UNIQUE, parent TEXT, comment TEXT, subReddit TEXT, unix INT, score INT)""")

#Format data in order to be tokenized
def formatData(data):
	data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
	return data

#For every comment we need to know it's parent body so we can know, for every answer, its question
def findParentBody(pid):
	try:
		sql = "SELECT comment FROM parentReply WHERE commentID = '{}' LIMIT 1".format(pid)
		cursor.execute(sql)
		result = cursor.fetchone()

		if result != None:
			return result[0]
		else: return False
	except Exception as e:
		print("findParentBody", str(e))
		return False

#Check if there is already an answer to the reply with score greater than the current one
def findExistingScore(pid):
	try:
		sql = "SELECT score FROM parentReply WHERE parentID = '{}' LIMIT 1".format(pid)
		cursor.execute(sql)
		result = cursor.fetchone()

		if result != None:
			return result[0]
		else: 
			return False
	except Exception as e:
		print("findExistingScore", str(e))
		return False

#If the comment is still available and a good candidate as answer
def acceptable(data):
	if len(data.split(' ')) > 50 or len(data) < 1 :
		return False
	elif len(data) > 1000:
		return False
	elif data == '[deleted]':
		return False
	elif data == '[removed]':
		return False
	else:
		return True

#We update the database and set the new value for the reply
def insertReplyAndReplace(commentID, parentID, parentData, body, subReddit, createdUTC, score):
	try:
		sql = """UPDATE parentReply SET parentID = "{}", commentID = "{}", parent = "{}",
				 comment = "{}", subReddit = "{}", unix = {}, score = {} WHERE parentID = {};""".format(parentID, commentID, parentData, body, subReddit, int(createdUTC), score, parentID)
		transactionBuild(sql)
	except Exception as e:
		print("insertReplyAndReplace", str(e))

#We insert the reply and the question. We got a valid pair
def insertReply(commentID, parentID, parentData, body, subReddit, createdUTC, score):
	try:
		sql = """INSERT INTO parentReply (parentID, commentID, parent,
				 comment, subReddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", {}, {});""".format(parentID, commentID, parentData, body, subReddit, int(createdUTC), score)
		transactionBuild(sql)
	except Exception as e:
		print("insertReply", str(e))

#We insert a new question
def insertQuestion(commentID, parentID, body, subReddit, createdUTC, score):
	try:
		sql = """INSERT INTO parentReply (parentID, commentID, comment,
				 subReddit, unix, score) VALUES ("{}", "{}", "{}", "{}", {}, {});""".format(parentID, commentID, body, subReddit, int(createdUTC), score)
		transactionBuild(sql)
	except Exception as e:
		print("insertQuestion", str(e))

#Start executing commands after more so that we don't have to begin a transaction every time. This way we save a lot of time
def transactionBuild(sql):
	global sqlTransaction
	sqlTransaction.append(sql)
	if len(sqlTransaction) > 1000:
		cursor.execute("BEGIN TRANSACTION")
		for command in sqlTransaction:
			try:
				cursor.execute(command)
			except Exception as e:
				pass
		connection.commit()
		sqlTransaction = []

if __name__ == '__main__':
	createTable()
	rows = 0 
	pairedRows = 0

	with open(FILE, buffering=1000) as file:
		for row in file:
			rows += 1
			row = json.loads(row)

			#Extract data from json
			parentID = row['parent_id'].split('_')[1]
			commentID = row['id']
			body = formatData(row['body'])
			createdUTC = row['created_utc'] 
			score = row['score']
			subReddit = row['subreddit']
			parentData = findParentBody(parentID)

			#Insertion logic
			if score >= 2:
				commentScore = findExistingScore(parentID)
				if commentScore:
					if score > commentScore and acceptable(body):
						#We have to replace the lower score comment with the better one
						insertReplyAndReplace(commentID, parentID, parentData, body, subReddit, createdUTC, score)
				else:
						if acceptable(body):
							if parentData:
								#If the current comment is a reply and the question doest have any answer
								insertReply(commentID, parentID, parentData, body, subReddit, createdUTC, score)
								pairedRows += 1
							else: 
								#If it's actually a question
								insertQuestion(commentID, parentID, body, subReddit, createdUTC, score)

			#See status sometimes				
			if rows % 1000 == 0:
				print("Total rows read: {} , paired rows: {}".format(rows, pairedRows))

			#After longer time, if we see that there are questions without reply, we just delete them cuz they are slowing the DB
			if rows > 0:
				if rows % 1000000 == 0:
					print("Start cleaning up")
					sql = "DELETE from parentReply WHERE parent is NULL"
					cursor.execute(sql)
					connection.commit()
					cursor.execute("VACUUM")
					connection.commit()

