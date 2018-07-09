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
	def __init__(self,new_files):
		self.get_Data(new_files)

	def get_Data(self,new_files):
		hold_data = []
		self.data = pd.DataFrame()
		if len(new_files) > 0:
			for item in new_files:
				with open(item, 'r') as file:
					sheet = pd.read_csv(file,index_col=False)
					hold_data.append(sheet)
			self.data = pd.concat(hold_data)
			return self.clean_Data()
		else:
			return None

	def clean_Data(self):
		self.data = self.data.rename(columns={'Posting Date':'postDate'})
		self.data['postDate'] = pd.to_datetime(self.data.postDate)
		self.data = self.data.sort_values(by='postDate')
		self.data['Index'] = range(self.data.shape[0])
		self.data = self.data.set_index('Index')
		self.data.drop(columns=to_drop,axis=1,inplace=True)
		self.data['Description'] = self.data['Description'].str.upper()
		self.data['Description'] = self.data['Description'].str.translate(str.maketrans(dict.fromkeys('0123456789/#-')))
		self.data['Description'] = self.data['Description'].str.split(r'\s{2,}')
		self.data['Description'] = self.data.apply(lambda x: pd.Series(x['Description'][0]),axis=1)
		for i,j in word_drop.items():
			self.data['Description'] = self.data['Description'].str.replace(i,j)
		return self.data


class DataBase():
	def __init__(self,database):
		self.concur_Database(database)
		self.create_Tables()
		self.validate_Statements()
		data = Data(self.new_files)
		#self.pd_Insert_Data(data.data, 'bankStatement')
		print(self.pull_Data('bankStatement', column='description'))

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
		self.new_files = []
		for file in files:
			if file in statements:
				continue
			else:
				self.insert_Data('existStatement', 'statement', file)
				self.new_files.append(file)
		print(f'{len(self.new_files)} new bank statements found:')
		for item in self.new_files:
			print(item)
		print('\n')

	#Insert data into database
	def insert_Data(self,table,column,data):
		if isinstance(data, list):
			for item in data:
				self.cur.execute(f'''INSERT INTO {table}({column}) VALUES(?)''',(item,))
		else:
			self.cur.execute(f'''INSERT INTO {table}({column}) VALUES(?)''',(data,))
		self.conn.commit()

	#Pandas insert data into database
	def pd_Insert_Data(self, data, table):
		data.to_sql(table,con=self.conn, if_exists='append', index=False)
		self.conn.commit()
		print('Data inserted into the ' + table + ' table')

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