import pymongo

from pprint import pprint

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

# show all database 
print(myclient.list_database_names())

mydb = myclient["mydb"] # database 
mycol = mydb["customers"] # collection

print(mydb.list_collection_names())


# insert
mydict = {
	"name" : "John",
	"address": "Highway 37"
}

x = mycol.insert_one(mydict)

print(x.inserted_id)

mylist = [
  { "name": "Amy", "address": "Apple st 652"},
  { "name": "Hannah", "address": "Mountain 21"},
  { "name": "Michael", "address": "Valley 345"},
  { "name": "Sandy", "address": "Ocean blvd 2"},
  { "name": "Betty", "address": "Green Grass 1"},
  { "name": "Richard", "address": "Sky st 331"},
  { "name": "Susan", "address": "One way 98"},
  { "name": "Vicky", "address": "Yellow Garden 2"},
  { "name": "Ben", "address": "Park Lane 38"},
  { "name": "William", "address": "Central st 954"},
  { "name": "Chuck", "address": "Main Road 989"},
  { "name": "Viola", "address": "Sideway 1633"}
]

x = mycol.insert_many(mylist)

print(x.inserted_ids)

for x in mycol.find():
	print(x)

print()


# You are not allowed to specify both 0 and 1 values in the same object (except if one of the fields is the _id field). 
# If you specify a field with the value 0, all other fields get the value 1, and vice versa:
# not display : 0 
# 
for x in mycol.find({}, {"_id": 0}):
	print(x)

# sort 
# -1 descending
# 1 ascending
mydoc = mycol.find().sort("name", -1)

for x in mydoc:
	print(x)

# update 
myquery = {"address": "Valley 345"}
new_values = { "$set": {"address":"Canyon 123"}}

mycol.update_one(myquery, new_values)
# x = mycol.update_many(myquery, new_values)
# print(x.modified_count, "documents updated ")


for x in mycol.find():
	print(x)


# delete 
myquery = { "address": "Mountain 21"} 
mycol.delete_one(myquery)

myquery = {"address" : {"$regex" : "^S"}}
x = mycol.delete_many(myquery)
print(x.deleted_count, " documents deleted ")

# delete all documents
x = mycol.delete_many({})
print(x.deleted_count, "documents deleted ")

# drop collection 
mycol.drop()