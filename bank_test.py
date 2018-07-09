import pandas as pd
import glob
import sqlite3

filePath = 'C:\\Users\\micha\\Budget\\Bank Data\\'
to_drop = ['Details', 'Type', 'Check or Slip #']
word_drop = {' IL':'', ' AURORA':'', ' LISLE':'', ' WOODRIDGE':'',
			 ' WHEATON':'', ' WESTMONT':'', ' LOMBARD':'', ' BLOOMINGDALE':'',
			 ' DOWNERS GROVE':'', ' NAPERVILLE':'', ' CHICAGO':''}

#Receive filepath to data awaiting insertion to database
def get_Filepath():
	files = []
	for item in glob.glob(filePath+'/*.csv'):
		files.append(item)
	return files

class Data():
	def __init__(self):
		pass

class DataBase():
	def __init__(self,database):
		self.concur_Database(database)
		self.create_Tables()
		self.validate_Statements()
		data = self.pull_Data('existStatement')
		for item in data:
			print(item)

	#Connect to and create cursor object for database
	def concur_Database(self,database):
		try:
			self.conn = sqlite3.connect(database)
			self.cur = self.conn.cursor()
			print(f'Connected to {database} database')
		except Error as e:
			print(e)
	
	#Create database tables
	def create_Tables(self):
		try:
			self.cur.execute('''CREATE TABLE IF NOT EXISTS bankStatement
								(ind INTEGER PRIMARY KEY,postDate DATE,description TEXT,amount FLOAT,balance FLOAT)''')
			self.cur.execute('''CREATE TABLE IF NOT EXISTS existStatement
							(statement TEXT)''')
			print('Tables created')
		except Error as e:
			print(e)

	#Validate that data does not already exist in database
	def validate_Statements(self):
		print('Validating...')
		statements = self.pull_Data('existStatement')
		files = get_Filepath()
		if len(statements) != 0:
			for file in files:
				if file in statements:
					continue
				else:
					self.insert_Data('existStatement', 'statement', file)
		else:
			self.insert_Data('existStatement', 'statement', files)
		print(f'{len(files)} bank statements found')

	#Insert data into database
	def insert_Data(self,table,column,data):
		if isinstance(data, list):
			for item in data:
				self.cur.execute(f'''INSERT INTO {table}({column}) VALUES(?)''',(item,))
		else:
			self.cur.execute(f'''INSERT INTO {table}({column}) VALUES(?)''',(data,))
		self.conn.commit()

	#Pull data from database
	def pull_Data(self,table,column='*'):
		self.cur.execute(f'SELECT {column} FROM {table}')
		data = self.cur.fetchall()
		data = self.format_Data(data)
		return data

	#Format data pulled from database and insert into list
	def format_Data(self, data):
		format_Data = []
		for item in data:
			format_Data.append(item[0])
		return format_Data

	#Delete database connection
	def __del__(self):
		self.conn.close()

if __name__ == '__main__':
	db = DataBase('BankInfo.db')