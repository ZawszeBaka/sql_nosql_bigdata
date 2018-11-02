import pandas as pd 

import random
from random import randint

import json 

import numpy as np

import mysql.connector
import string
import pymongo

import time

import sys

from pprint import pprint

'''
	Update new order into database 

	orders 
	{
		"id": 1234567,
		"idCustomer": 45,
		"orderProducts": [
			{
				"idProduct": 566,
				"price": 32.45, 
				"productName":"Carroway Seed"
			}
		],
		"shippingAddress": {"city": "Burtrask Posale", "street":"23 Mandrake" , "12500"}
	}

'''

print(' [INFO] Update new order into database')


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
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydb"] # database 
mycol = mydb["order"]

mycol.delete_one({"_id": 1234567}) 

print(' [INFO] MongoDB: ')
value = {"_id": 1234567,
	"idCustomer": 45,
	"orderProducts": [
		{
			"idProduct": 3333,
			"price": 32.22, 
			"productName":"Carroway Seed 2"
		},
		{
			"idProduct": 2222,
			"price": 12.45, 
			"productName":"Fresh Milk"
		}
	],
	"shippingAddress": {"city": "Burtrask Posale", "street":"23 Mandrake" , "postalCode": "12500"}}
start_time = time.time()
mycol.insert_one(value)
print(" [INFO] Time Execution: %s seconds ---" % (time.time() - start_time))
print('\n [INFO] MongoDB Insert into "order"  1 record(s) ')
pprint(value)



print('---------------------------------------------------------------------------------------------------')

# customer(idCustomer, name, idBillingAddress)
# billingAddress(idBillingAddress, street, city,  postalCode) 

# orders(idOrder, idCustomer, idShippingAddress)   # idShippingAddress corresponding to idBillingAddress
# orderProduct(idOrder, idProduct) # one order can have multiple products
# product(idProduct, price, productName)
mydb = mysql.connector.connect(host="localhost", 
								   user="root", 
								   passwd="",
								   database = "mydatabase")
mycursor = mydb.cursor()
query1 = " INSERT INTO orders (idOrder, idCustomer, idShippingAddress) VALUES (%s, %s, %s) "
value1 = (1234567, 45, 7777 )
query2 = " INSERT INTO billingAddress(idBillingAddress, street, city, postalCode) VALUES (%s, %s, %s,%s)"
value2 = (7777, "23 Mandrake", "Burtrask Posale", "12500" )
query3 = " INSERT INTO orderProduct(idOrder, idProduct) VALUES (%s, %s)"
value3 = [(45, 3333),(45,2222)]
query4 = " INSERT INTO product(idProduct, price, productName) VALUES (%s, %s, %s)"
value4 = [(3333, 32.22, "Carroway Seed 2"),(2222,12.45, "Fresh Milk")]

print("\n [QUERY] MySQL:\n", query1 % value1 , '\n', query2 % value2 , '\n', query3, '\n', query4)
start_time = time.time()
mycursor.execute(query1,value1)
mycursor.execute(query2,value2)
mycursor.executemany(query3,value3)
mycursor.executemany(query4,value4)
print(" [INFO] Time Execution: %s seconds ---" % (time.time() - start_time))
mydb.commit()
print(' [INFO] ', mycursor.rowcount, " records were inserted")


