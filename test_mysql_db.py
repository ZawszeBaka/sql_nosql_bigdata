
import mysql.connector

# helping function 
def querying(cursor, query):

	print('\n [QUERY] ', query)
	cursor.execute(query)
	for x in cursor: 
		print(x)

	print()


mydb = mysql.connector.connect(host="localhost", 
							   user="root", 
							   passwd="")

"""
	Scenario: 
		Delete Database "mydatabase"
		Create new Database called "mydatabase"
		Show all databases 

		Connect to mysql and database "mydatabase"
		Create new Table called "customer" and column names with initalized condition

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

# Create a table "customers" with primary key "id" 
query = """CREATE TABLE customers (  
	id INT AUTO_INCREMENT PRIMARY KEY, 
	name VARCHAR(255), 
	address VARCHAR(255)
)"""
print("\n [QUERY] ", query)
mycursor.execute(query)

# Show tables
query = "SHOW TABLES "
print("\n [QUERY] ", query)
mycursor.execute(query)
for x in mycursor:
	print(x)

# If the table already exists, use the "ALTER TABLE" keyword
# Add new column named "id" with some properties : INT, AUTO_INCREMENT, PRIMARY KEY 
#mycursor.execute("ALTER TABLE customers     ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY") 

## Insert a record in the "customers" table:
query = "INSERT INTO customers (name, address) VALUES (%s, %s)"
val = ("John", "Highway 21")
print("\n [QUERY] " +query % val )
mycursor.execute(query,val)

mydb.commit() # It is required to make the changes, otherwise no changes are made to the table 

print(' [INFO] ', mycursor.rowcount, " record inserted.")

## Insert Multiple Rows  executemany() method
query = "INSERT INTO customers (name, address) VALUES (%s, %s)"
print('\n [QUERY] ' + query )
val = [ ('Peter', 'Lowstreet 4'),
		('Amy', 'Apple st 652'),
		('Hannah', 'Mountain 21'),
		('Michael', 'Valley 345'),
		('Sandy', 'Ocean blvd 2'),
		('Betty', 'Green Grass 1'),
		('Richard', 'Sky st 331'),
		('Susan', 'One way 98'),
		('Vicky', 'Yellow Garden 2'),
		('Ben', 'Park Lane 38'),
		('William', 'Central st 954'),
		('Chuck', 'Main Road 989'),
		('Viola', 'Sideway 1633')]
mycursor.executemany(query, val)
mydb.commit()
print(' [INFO] ', mycursor.rowcount, " records were inserted")

## Get inserted ID 
# You can get the id of the row you just inserted by asking the cursor object
# If you insert more than one row, the id of the last inserted row is returned 
print(" [INFO] The id of the row you just inserted or the one of the last inserted row is :", mycursor.lastrowid )


## SELECT from a TABLE 
# Select all records from the "customers" table, and display the result 
query = "SELECT * FROM customers"
print('\n [QUERY] ' + query )
mycursor.execute(query)
myresult = mycursor.fetchall() # which fetches all rows from the last executed statement
for x in myresult:
	print(x)

## SELECTing Columns 
# Select only the name and address columns 
query = "SELECT name, address FROM customers"
print('\n [QUERY] ' + query )
mycursor.execute(query)
myresult = mycursor.fetchall() # which fetches all rows from the last executed statement
for x in myresult:
	print(x)

#Note:
# fetchall() : fetches all rows from the last executed statement
# fetchone() : fetches the first row from the last executed statement


## Wildcard Characters
# Select the records that starts, includes, or ends with a given letter or phrase
# Use the % to represent wildcard characters

# Select records where the address contains the word "way"
query = "SELECT * FROM customers WHERE address LIKE '%way%'"
print('\n [QUERY] ' + query )
mycursor.execute(query)
myresult = mycursor.fetchall() # which fetches all rows from the last executed statement
for x in myresult:
	print(x)

## Prevent SQL Injection 
# When query values are provided by the user, you should escape the values
# This is to prevent SQL injections, which is a common web hacking technique to 
# destroy or misuse your database 
# The mysql.connector module has methods to escape query values

# Escape query values by using the placeholder %s method 
query = "SELECT * FROM customers WHERE address = %s"
adr = ("Yellow Garden 2", )
print('\n [QUERY] ' + query % adr )
mycursor.execute(query, adr)
myresult = mycursor.fetchall() # which fetches all rows from the last executed statement
for x in myresult:
	print(x)

## Sort the Result 
# ORDER BY : ascending by default
# ORDER BY DESC : descending 
query = "SELECT * FROM customers ORDER BY name "
print('\n [QUERY] ' + query )
mycursor.execute(query)
myresult = mycursor.fetchall() # which fetches all rows from the last executed statement
for x in myresult:
	print(x)

## Delete Record 
# Delete any record where the address is "Mountain 21"
query = "DELETE FROM customers WHERE address = 'Mountain 21'"
print('\n [QUERY] ' + query )
mycursor.execute(query)
mydb.commit()
print(' [INFO] ',mycursor.rowcount, "record(s) deleted")

## Prevent SQL Injection 
# It is considered a good practice to escape the values of any query, also in delete statements
# This is to prevent SQL injections, which is a common web hacking technique to destroy 
# or misuse your database 
# The mysql.connector module uses the placeholder %s to escape values in the delete statement 

# Escape values by using the placeholder %s method :
query = "DELETE FROM customers WHERE address = %s"
adr = ("Yellow Garden 2", )
print('\n [QUERY] ' + query % adr)
mycursor.execute(query, adr)
mydb.commit()
print(' [INFO] ',mycursor.rowcount, "record(s) deleted")


## Update Table 
# Overwrite the address column from "Valley 345" to "Canyoun 123"
query = "UPDATE customers SET address = 'Canyon 123' WHERE address = 'Valley 345'"
print('\n [QUERY] ' + query)
mycursor.execute(query)
mydb.commit()
print(' [INFO] ',mycursor.rowcount, "record(s) affected ")

## Prevent SQL Injection (UPDATE)
# It is considered a good practice to escape the values of any query 
# also in update statements 
query = "UPDATE customers SET address = %s WHERE address = %s"
val = ("Valley 345", "Canyon 123")
print('\n [QUERY] ' + query % val )
mycursor.execute(query, val)
mydb.commit()
print(' [INFO] ',mycursor.rowcount, "record(s) affected ")

## Limit the Result 
# Limit the number of records returned from the query
# Select the 5 first records in the "customers" table 
query = "SELECT * FROM customers LIMIT 5"
print('\n [QUERY] ' + query)
mycursor.execute(query)
for x in mycursor.fetchall():
	print(x)

## Start from Another Position 
# Return 5 records , starting from the third record 
query = "SELECT * FROM customers LIMIT 5 OFFSET 2"
print('\n [QUERY] ' + query)
mycursor.execute(query)
for x in mycursor.fetchall():
	print(x)

## Join 2 or more Tables 
# Combine roes from two or more tables, based on a related column
# between them
# Consider you have a "users" table and a "products" table 
query = """
	CREATE TABLE users (  
		id INT AUTO_INCREMENT PRIMARY KEY, 
		name VARCHAR(255), 
		fav INT
	)
"""
print('\n [QUERY] ' + query )
mycursor.execute(query)

query = """
	CREATE TABLE products (  
		id INT AUTO_INCREMENT PRIMARY KEY, 
		name VARCHAR(255)
	)
"""
print('\n [QUERY] ' + query )
mycursor.execute(query)

# Show tables
query = "SHOW TABLES "
print("\n [QUERY] ", query)
mycursor.execute(query)
for x in mycursor:
	print(x)

# query = "INSERT INTO users (name, fav) VALUES (%s, %d)"
# print('\n [QUERY] ' + query)
# val = [ ('John', 154),
# 		('Peter', 154),
# 		('Amy', 155),
# 		('Hannah',1),
# 		('Michael',10) ]
# mycursor.executemany(query, val)
# mydb.commit()
# print(' [INFO] ', mycursor.rowcount, " records were inserted")

query = "INSERT INTO products (name) VALUES (%s)"
print('\n [QUERY] ' + query)
val = [ ('Chocolate Heaven'),
		('Tasty Lemons'),
		('Vanilla Dreams')]
mycursor.executemany(query, val)
mydb.commit()
print(' [INFO] ', mycursor.rowcount, " records were inserted")


print("\n [QUERY] ", query)
mycursor.execute(query)








# Show tables
query = "SHOW TABLES "
print("\n [QUERY] ", query)
mycursor.execute(query)
for x in mycursor:
	print(x)

## Delete a Table 
query = "DROP TABLE customers"
print('\n [QUERY] ' + query)
mycursor.execute(query)

# Show tables
query = "SHOW TABLES "
print("\n [QUERY] ", query)
mycursor.execute(query)
for x in mycursor:
	print(x)

## Drop only if exist
# If the table you want to delete is already deleted, or for any other reason does not 
# exist , you can use the IF EXISTS keyword to avoid getting an error

# Delete the table "customers" if it exists:
query = "DROP TABLE IF EXISTS customers"
print('\n [QUERY] ' + query)
mycursor.execute(query)
for x in mycursor:
	print(x)

# Show tables
query = "SHOW TABLES "
print("\n [QUERY] ", query)
mycursor.execute(query)
for x in mycursor:
	print(x)