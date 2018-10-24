
import pymongo
import pandas as pd

PATH = 'dataset/1500000_Sales_Records/'
PATH_DATA_1 = 'dataset/1500000_Sales_Records/' + '1500000_Sales_Records.csv'

def segment_big_file( PATH_DATA ,NUM_BLOCKS = 250000):
	
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
		if end > t.shape[0]:
			break

	log_path = PATH+'segment_paths.csv'
	print(' [LOGGING] ' , PATH+log_path)
	pd.DataFrame(_log).to_csv(log_path)

def put_into_database(df, 
					  database_name = 'Sales' , 
					  collection_name = 'Records'):
	
	myclient = pymongo.MongoClient("mongodb://localhost:27017/")
	mydb = myclient["mydb"]
	mycol = mydb["customers"]
	# # insert
	# mydict = {
	# 	"name" : "John",
	# 	"address": "Highway 37"
	# }

	# x = mycol.insert_one(mydict)
	
	mydb.drop_collection("customers")
	print(mydb.list_collection_names())


	




if __name__ == '__main__':
	
	# uncomment this line to segment huge amount of data in 1 file into multiple files 
	# segment_big_file(PATH_DATA_1)

	put_into_database(0)




# [{'col1': 1.0, 'col2': 0.5}, {'col1': 2.0, 'col2': 0.75}]
# t_dict = t.to_dict('records')

# 

# # show all database 
# print(myclient.list_database_names())

# mydb = myclient["mydb"] # database 
# mycol = mydb["customers"] # collection

# print(mydb.list_collection_names())
