import sys
import urllib.request
from pyspark import SparkConf, SparkContext
from operator import add
import mysql.connector

# Spark Configure Setup
conf = SparkConf().setMaster("local").setAppName("Spark Wordcount")
sc = SparkContext.getOrCreate(conf = conf)
sc.setLogLevel("ERROR")

# SETUP
try:
    gutenberg_file = urllib.request.urlopen(sys.argv[1]).read().decode()
except:
    gutenberg_file = sys.argv[1]

raw_data = sc.textFile(gutenberg_file) #Read the input text file
word_rdd = raw_data.flatMap(lambda l: l.split(" ")) #Collect words into RDD

#MAPPER
word_rdd = word_rdd.map(lambda word: (word, 1))

#REDUCER
final_count = word_rdd.reduceByKey(add).collect()

#Write to NoSQL Mongo Database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Hidden-Password",
  database = "wordcount"
)
mycursor = mydb.cursor()

#sql = "CREATE DATABASE wordcount"
#mycursor.execute(sql)
#sql = "CREATE TABLE wc (word VARCHAR(255), count VARCHAR(255))"
#mycursor.execute(sql)
sql = "INSERT INTO wc (word, count) VALUES (%s, %s)"
mycursor.executemany(sql, final_count)
mydb.commit()
print(mycursor.rowcount, "was inserted.")
mycursor.execute("SELECT * FROM wc")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
