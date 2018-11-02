import pandas as pd 

import random
from random import randint

import json 

import numpy as np

import mysql.connector
import string
import pymongo


myclient = pymongo.MongoClient("mongodb://localhost:27017/")


FIRST_NAMES_PATH = 'random_samples/1000_first_name.csv'
LAST_NAMES_PATH = 'random_samples/1000_last_name.csv'
PRODUCT_NAMES_PATH = 'random_samples/1000_product_name.csv'
ADDRESSES_PATH = 'random_samples/1000_city_street_postalCode.csv'

OUT_NOSQL_PATH = 'dataset/nosql' # + ... multiple json file 
OUT_SQL_PATH = 'dataset/sql' # + ... multiple csv file 

# generating 10000 customers, 1000000 orders 
#
# generating billingAddress:
# 1000 => all 

# generating 


# Read sample files :
first_name_samples = pd.read_csv(FIRST_NAMES_PATH, encoding = 'utf-8')
last_name_samples = pd.read_csv(LAST_NAMES_PATH, encoding = 'utf-8')
product_name_samples = pd.read_csv(PRODUCT_NAMES_PATH, encoding = 'utf-8')
address_samples = pd.read_csv(ADDRESSES_PATH, encoding = 'utf-8')

ROWS = 1000 ; # Be careful if you change data from random_samples 

def random_int_from_to(_min, _max):
	return randint(_min, _max) 

def random_multi_int_from_to(num, _min, _max):
	randList = []
	for i in range(num):
		randList.append(randint(_min, _max))
	return randList

def random_float_from_to(_min, _max):
	return random.uniform(_min, _max)

def random_multi_float_from_to(num, _min, _max):
	randList = []
	for i in range(num):
		randList.append(random.uniform(_min, _max))
	return randList

def generating_random(num_customers = 10000 , num_orders = 1000000 ):

	# NoSQL
	'''
	customers
	{
		"id": 1,
		"name": "paul",
		"billingAddress": {"city": "Chicago"}
	}

	orders 
	{
		"id": 99,
		"idCustomer": 1,
		"orderProducts": [
			{
				"idProduct": 127,
				"price": 32.45,
				"productName":"NoSql sdas"
			}
		],
		"shippingAddress": {"city": "Chicago", "street" , "postalCode"}

	}

	'''
	customersNoSQL = []
	ordersNoSQL = []

	# SQL 
	# 
	# customer(idCustomer, name, idBillingAddress)
	# billingAddress(idBllingAddress, street, city,  postalCode) 
	# 
	#
	# order(idOrder, idCustomer, idShippingAddress)   # idShippingAddress corresponding to idBillingAddress
	# orderProduct(idOrder, idProduct) # one order can have multiple products
	# product(idProduct, price, productName)
	customersSQL = []
	# billingAddress

	ordersSQL = []
	orderProductsSQL = []
	productsSQL = []


	##########################################

	# SQL : billingAddress(idBllingAddress, street, city,  postalCode)
	# id from 1 to 1000
	billingAddressSQL = address_samples
	billingAddressSQL['idBillingAddress'] = list(range(1,ROWS+1))

	# SQL: product(idProduct, price, name)
	lst_price = random_multi_float_from_to(ROWS, 1,1000)
	product_name_samples['price'] = [ '%.2f' % each_price for each_price in lst_price]
	product_name_samples['idProduct'] = range(1,ROWS+1)

	for idCustomer in range(1,num_customers+1):

		if idCustomer % 1000 == 0 :
			print('customer', idCustomer , '/', num_customers)

		###  SQL: customer(idCustomer, name, idBillingAddress) 

		# random name = random Id first name + random Id last name 
		fnID = random_int_from_to(1,ROWS)
		lnID = random_int_from_to(1,ROWS)
		name = first_name_samples.iloc[fnID-1].fname + last_name_samples.iloc[lnID-1].lname

		# random billingAddress
		idBillingAddress = random_int_from_to(1,ROWS)

		# SQL: customer
		customersSQL.append((idCustomer , name, idBillingAddress ))
		

		###  NoSQL
		city = address_samples['city'].iloc[idBillingAddress-1]
		street = address_samples['street'].iloc[idBillingAddress-1]
		postalCode = address_samples['postalCode'].iloc[idBillingAddress-1]

		billingAddress = dict()

		if not pd.isnull(city):
			billingAddress['city'] = city
		if not pd.isnull(street):
			billingAddress['street'] = street
		if not pd.isnull(postalCode):
			billingAddress['postalCode'] = postalCode

		customersNoSQL.append({"_id": idCustomer, "name": name, "billingAddress": billingAddress})


	# Orders 
	for idOrder in range(1,num_orders+1):

		if idOrder % 10000 == 0 :
			print('order', idOrder , '/', num_orders)

		### SQL : order(idOrder, idCustomer, idShippingAddress)   # idShippingAddress corresponding to idBillingAddress

		# random idCustomer, idShippingAddress
		cusID = random_int_from_to(1,num_customers)
		addrID = random_int_from_to(1, ROWS)
		ordersSQL.append((idOrder, cusID, addrID))

		# orderProduct(idOrder, idProduct) # one order can have multiple products
		numProducts = random_int_from_to(1,10)
		prodsID = random_multi_int_from_to(numProducts, 1, ROWS)

		for idProduct in prodsID:
			orderProductsSQL.append((idOrder, idProduct))

		### NoSql
		eachOrder = [] # each order can have multiple products 
		for idProduct in prodsID:
			productName = product_name_samples['productName'].iloc[idProduct-1] 
			price =  product_name_samples['price'].iloc[idProduct-1]
			eachOrder.append({"id": idProduct, "productName": productName, "price": price})

		city = address_samples['city'].iloc[addrID-1]
		street = address_samples['street'].iloc[addrID-1]
		postalCode = address_samples['postalCode'].iloc[addrID-1]

		shippingAddress = dict()

		if not pd.isnull(city):
			shippingAddress['city'] = city
		if not pd.isnull(street):
			shippingAddress['street'] = street
		if not pd.isnull(postalCode):
			shippingAddress['postalCode'] = postalCode

		ordersNoSQL.append({"_id": idOrder, "idCustomer": cusID, "orderProducts": eachOrder, "shippingAddress": shippingAddress})

	def fix_letter(p):
		return ''.join(x for x in p if x in string.printable)
	
	# SQL: Convert to DataFrame . Update: No convert 
	# customersSQL = pd.DataFrame(customersSQL, columns = ['idCustomer','name', 'idBillingAddress' ])
	billingAddressSQL.fillna('', inplace = True )
	billingAddressSQL = [tuple([x[0],fix_letter(x[1]), fix_letter(x[2]) , x[3]]) for x in billingAddressSQL[['idBillingAddress', 'street', 'city', 'postalCode']].values.tolist()]  # [idBillingAddress, street, city, , postalCode, ]

	# ordersSQL = pd.DataFrame(ordersSQL, columns = ['idOrder','idCustomer', 'idShippingAddress'])
	# orderProductsSQL = pd.DataFrame(orderProductsSQL, columns = ['idOrder', 'idProduct'])
	productsSQL = product_name_samples # [idProduct, 'productName', "price", ]
	productsSQL = [tuple(x) for x in productsSQL[['idProduct', 'productName', 'price']].values.tolist()]  

	print(customersSQL[:5])


	# SAVE TO DATABASE 
	mydb = mysql.connector.connect(host="localhost", 
							   user="root", 
							   passwd="")

	"""
		Scenario: 
			Delete Database "mydatabase"
			Create new Database called "mydatabase"
			Show all databases 

			Connect to mysql and database "mydatabase"
			Create new TableS: 
	"""


	mycursor = mydb.cursor()

	# Delete Database mydatabase 
	query = "DROP DATABASE mydatabase"
	print("\n [QUERY] "+ query)
	mycursor.execute(query)

	# Create Database mydatabase 
	query = "CREATE DATABASE mydatabase"
	print("\n [QUERY] " + query)
	mycursor.execute(query)

	# Show Databases 
	query = "SHOW DATABASES"
	print("\n [QUERY] " + query)
	mycursor.execute(query)
	for x in mycursor:
		print(x)

	## Connecting to the database "mydatabase"
	mydb = mysql.connector.connect(host="localhost", 
								   user="root", 
								   passwd="",
								   database = "mydatabase")

	mycursor = mydb.cursor()

	# customer(idCustomer, name, idBillingAddress)
	# billingAddress(idBllingAddress, street, city,  postalCode) 

	# order(idOrder, idCustomer, idShippingAddress)   # idShippingAddress corresponding to idBillingAddress
	# orderProduct(idOrder, idProduct) # one order can have multiple products
	# product(idProduct, price, productName)

	# Create a table "customer"
	query = """CREATE TABLE customer (  
		idCustomer INT PRIMARY KEY, 
		name VARCHAR(255), 
		idBillingAddress VARCHAR(255)
	)"""
	print("\n [QUERY] ", query)
	mycursor.execute(query)

	# Create a table "billingAddress"
	query = """CREATE TABLE billingAddress (  
		idBillingAddress INT PRIMARY KEY, 
		street VARCHAR(255), 
		city VARCHAR(255),
		postalCode VARCHAR(255)
	)"""
	print("\n [QUERY] ", query)
	mycursor.execute(query)

	# Create a table "orders" . Be careful "order" is one of keyword in SQL
	query = """CREATE TABLE orders (  
		idOrder INT PRIMARY KEY, 
		idCustomer INT, 
		idShippingAddress INT
	)"""
	print("\n [QUERY] ", query)
	mycursor.execute(query)

	# Create a table "orderProduct"
	query = """CREATE TABLE orderProduct (  
		idOrder INT , 
		idProduct INT
	)"""
	print("\n [QUERY] ", query)
	mycursor.execute(query)

	# Create a table "product"
	query = """CREATE TABLE product (  
		idProduct INT PRIMARY KEY, 
		price VARCHAR(255), 
		productName VARCHAR(255)
	)"""
	print("\n [QUERY] ", query)
	mycursor.execute(query)

	# Show tables
	query = "SHOW TABLES "
	print("\n [QUERY] ", query)
	mycursor.execute(query)
	for x in mycursor:
		print(x)

	## Insert Multiple Rows  executemany() method
	query = "INSERT INTO customer (idCustomer, name, idBillingAddress) VALUES (%s, %s, %s)"
	print('\n [QUERY] ' + query )
	mycursor.executemany(query, customersSQL)
	mydb.commit()
	print(' [INFO] ', mycursor.rowcount, " records were inserted")

	## Insert Multiple Rows  executemany() method
	query = "INSERT INTO billingAddress (idBillingAddress, street, city,  postalCode) VALUES (%s, %s, %s, %s)"
	print('\n [QUERY] ' + query )
	mycursor.executemany(query, billingAddressSQL)
	mydb.commit()
	print(' [INFO] ', mycursor.rowcount, " records were inserted")

	## Insert Multiple Rows  executemany() method
	query = "INSERT INTO orders (idOrder, idCustomer, idShippingAddress) VALUES (%s, %s, %s)"
	print('\n [QUERY] ' + query )
	mycursor.executemany(query, ordersSQL)
	mydb.commit()
	print(' [INFO] ', mycursor.rowcount, " records were inserted")

	## Insert Multiple Rows  executemany() method
	query = "INSERT INTO orderProduct(idOrder, idProduct) VALUES (%s, %s)"
	print('\n [QUERY] ' + query )
	mycursor.executemany(query, orderProductsSQL)
	mydb.commit()
	print(' [INFO] ', mycursor.rowcount, " records were inserted")

	## Insert Multiple Rows  executemany() method
	query = "INSERT INTO product (idProduct, productName, price) VALUES (%s, %s, %s)"
	print('\n [QUERY] ' + query )
	mycursor.executemany(query, productsSQL)
	mydb.commit()
	print(' [INFO] ', mycursor.rowcount, " records were inserted")






	# NoSQL: 
	# customersNoSQL
	# ordersNoSQL

	mydb = myclient["mydb"] # database 
	mycol = mydb["customer"] # collection
	mycol.drop() 
	mycol = mydb["order"] # collection
	mycol.drop() 

	# customer
	mycol = mydb["customer"]

	x = mycol.insert_many(customersNoSQL)
	print('\n [INFO] MongoDB Insert into customer : ', len(x.inserted_ids))

	# order
	mycol = mydb["order"]

	x = mycol.insert_many(ordersNoSQL)
	print('\n [INFO] MongoDB Insert into order : ', len(x.inserted_ids))




	# SAVE TO FILE 
	# Sql
	# customersSQL.to_csv(OUT_SQL_PATH + '/customer.csv', index = False)
	# billingAddressSQL.to_csv(OUT_SQL_PATH + '/billingAddress.csv', index = False)
	# ordersSQL.to_csv(OUT_SQL_PATH + '/order.csv', index = False)
	# orderProductsSQL.to_csv(OUT_SQL_PATH + '/orderProduct.csv',  index = False )
	# productsSQL.to_csv(OUT_SQL_PATH + '/product.csv',  index = False)
	# billingAddressSQL.fillna('', inplace = True )
	
	# customersSQL = customersSQL.to_dict('records')
	# billingAddressSQL = billingAddressSQL.to_dict('records')
	# ordersSQL = ordersSQL.to_dict('records')
	# orderProductsSQL = orderProductsSQL.to_dict('records')
	# productsSQL = productsSQL.to_dict('records')

	# def default(o):
	#     if isinstance(o, np.int64): return int(o)  
	#     raise TypeError

	# customersSQL = json.dumps(customersSQL,sort_keys=True, indent=4, separators = (',', ':'))
	# billingAddressSQL = json.dumps(billingAddressSQL,sort_keys=True, indent=4, separators = (',', ':'))
	# ordersSQL = json.dumps(ordersSQL,sort_keys=True, indent=4, separators = (',', ':'), default = default)
	# orderProductsSQL = json.dumps(orderProductsSQL,sort_keys=True, indent=4, separators = (',', ':'), default = default)
	# productsSQL = json.dumps(productsSQL,sort_keys=True, indent=4, separators = (',', ':'), default = default)

	# with open(OUT_SQL_PATH + '/customer.json', 'w', encoding='utf8') as f : 
	# 	f.write(customersSQL)

	# with open(OUT_SQL_PATH + '/billingAddress.json', 'w', encoding='utf8') as f : 
	# 	f.write(billingAddressSQL)

	# with open(OUT_SQL_PATH + '/order.json', 'w', encoding='utf8') as f : 
	# 	f.write(ordersSQL)

	# with open(OUT_SQL_PATH + '/orderProduct.json', 'w', encoding='utf8') as f : 
	# 	f.write(orderProductsSQL)

	# with open(OUT_SQL_PATH + '/product.json', 'w', encoding='utf8') as f : 
	# 	f.write(productsSQL)


	# # NoSQL
	# customersNoSQL = json.dumps(customersNoSQL,sort_keys=True, indent=4, separators = (',', ':'))
	# ordersNoSQL = json.dumps(ordersNoSQL,sort_keys=True, indent=4, separators = (',', ':'))

	# with open(OUT_NOSQL_PATH + '/customer.json', 'w') as f :
	# 	f.write(customersNoSQL)

	# with open(OUT_NOSQL_PATH + '/order.json', 'w') as f :
	# 	f.write(ordersNoSQL)



if __name__ == '__main__':
	#generating_random(num_customers = 10000, num_orders = 1000000)
	generating_random(num_customers = 1000, num_orders = 1000) 