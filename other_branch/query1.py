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
	Find the name, the billing address of a customer whose id is 20 
'''
CUSTOMERID = 20 
try:
	CUSTOMERID = sys.argv[1]
except:
	pass

print(' [INFO] Find the name, the billing address of a customer whose id is ', CUSTOMERID)


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

print(' [INFO] MongoDB: ')
start_time = time.time()
mycol = mydb["order"]
my_order = mycol.find({"idCustomer": int(CUSTOMERID)}, {"orderProducts" : 1 , "_id": 0})

mycol = mydb["customer"]
my_customer = mycol.find({"_id" : 10}, {"name" : 1 , "_id": 0})
print(" [INFO] Time Execution: %s seconds ---" % (time.time() - start_time))

for x in my_customer:
	pprint(x)

for x in my_order:
	pprint(x)




print('---------------------------------------------------------------------------------------------------')

# customer(idCustomer, name, idBillingAddress)
# billingAddress(idBllingAddress, street, city,  postalCode) 

# orders(idOrder, idCustomer, idShippingAddress)   # idShippingAddress corresponding to idBillingAddress
# orderProduct(idOrder, idProduct) # one order can have multiple products
# product(idProduct, price, productName)
mydb = mysql.connector.connect(host="localhost", 
								   user="root", 
								   passwd="",
								   database = "mydatabase")
mycursor = mydb.cursor()
query = """
	SELECT customer.name, product.productName, billingAddress.street, billingAddress.city, billingAddress.postalCode
	FROM orders , orderProduct, product, billingAddress , customer
	WHERE orders.idCustomer = %s AND orders.idOrder = orderProduct.idOrder 
		AND orders.idCustomer = customer.idCustomer
		AND product.idProduct = orderProduct.idProduct
		AND orders.idShippingAddress = billingAddress.idBillingAddress
""" % CUSTOMERID
print("\n [QUERY] MySQL:", query)
start_time = time.time()
mycursor.execute(query)
print(" [INFO] Time Execution: %s seconds ---" % (time.time() - start_time))
for x in mycursor.fetchall():
	print(x)



