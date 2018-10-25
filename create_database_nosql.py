
import os

import pymongo
import pandas as pd

PATH = 'dataset/1500000_Sales_Records/'

def segment_big_file( PATH_DATA ,NUM_BLOCKS = 250000):

	'''
		Input:
			PATH_DATA : path to the big big big file 

		Process:
			Divide into blocks . Each blocks has NUM_BLOCKS rows
			Save to separated files 

	'''
	
	print(' [INFO] reading '+ PATH_DATA)

	# 	  col1  col2
	# a     1   0.50
	# b     2   0.75
	t = pd.read_csv(PATH_DATA)
	print(' [INFO] Shape : ' , t.shape)

	# logging
	_log = []

	# divide into blocks
	begin,end = 0, NUM_BLOCKS
	while True :

		path_name = PATH + 'segment_' + str(end) + '.csv'
		_log.append([path_name])
		t.iloc[begin: end].to_csv(path_name)
		print(' [SEGMENT FILE] ' + path_name + ' is saved ' )

		begin += NUM_BLOCKS
		end += NUM_BLOCKS
		if end > t.shape[0] and begin < t.shape[0]:
			end = t.shape[0]
		elif end > t.shape[0]:
			break
		else:
			continue

	log_path = PATH+'segment_paths.csv'
	print(' [LOGGING] ' , PATH+log_path)
	pd.DataFrame(_log).to_csv(log_path)


def save_segments_to_mongodb(segment_directory_path, 
						    database_name , 
						    collection_name,
						    PORT = 27017 ):
	'''
		Input  
			database_name: Database name 
			collection_name : Collection name 
			PORT : default is 27017 for MongoDB
	'''
	
	# MongoDB	
	myclient = pymongo.MongoClient("mongodb://localhost:%d/"%(PORT))

	# Access Database
	mydb = myclient[database_name]
	# print(mydb.list_collection_names())

	# Reset collection whose name is "collection_name"
	mydb.drop_collection(collection_name)

	# Create new collection with the name "collection_name"
	mycol = mydb[collection_name]

	# Read data 
	segment_paths = [ path for path in os.listdir(segment_directory_path) if "segment" in path and "paths" not in path]
	segment_paths = list(sorted(segment_paths, key = lambda x: int(x.split(".")[0].split("_")[1])))
	print(' [INFO] All segment paths', segment_paths)

	# Insert data 
	for segment_path in segment_paths:
		print('\n\n [INFO] Reading csv file', PATH+segment_path)
		df = pd.read_csv(PATH+segment_path)
		
		# [{'col1': 1.0, 'col2': 0.5}, {'col1': 2.0, 'col2': 0.75}]
		print(' [INFO] Converting from DataFrame into List of directories... ')
		df = df.to_dict('records')

		print(' [INFO] Inserting into database ... ')
		mycol.insert_many(df)




if __name__ == '__main__':
	
	## Uncomment this line to segment huge amount of data in 1 file into multiple files 
	# path_data_1 = 'dataset/1500000_Sales_Records/' + '1500000_Sales_Records.csv'
	# segment_big_file(path_data_1, NUM_BLOCKS = 50000)

	## Save to Mongodb
	# segment_directory_path = 'dataset/1500000_Sales_Records/'
	# save_segments_to_mongodb(segment_directory_path, 'Demo', 'Sales')

