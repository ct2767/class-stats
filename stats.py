#!/usr/bin/env/python3

import sqlite3
import math
from sqlite3 import Error

# connecting to database
def create_connection(db_file):
	try:
		conn = sqlite3.connect(db_file)
		return conn
	except Error as e:
		print(e)
	return None

def return_table_names(conn):
	cur = conn.cursor()
	cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
	table_list = [row[0] for row in cur.fetchall()]
	
	return table_list

def get_scores(conn):
	cur = conn.cursor()
	cur.execute("SELECT TOTAL FROM '{tn}'".format(tn=table_name))
	
	result_list = [row[0] for row in cur.fetchall()]

	return result_list

def mean(conn, use_database_scores=True, list_to_use=[]):
	
	if (use_database_scores):
		scores = get_scores(conn)
	else:
		scores = list_to_use

	total = 0

	for element in scores:
		total += element

	total /= len(scores)

	return total

def mode(conn):
	scores = get_scores(conn)	

	mode = max(set(scores), key=scores.count)

	return mode

def std_dev(conn):
	scores = get_scores(conn)
	
	first_mean  = mean(conn)
	
	for element in scores:
		element -= first_mean
		element = element ** 2
	
	std_dev = math.sqrt(mean(conn, False, scores))
	
	return std_dev

def find_duplicates(user_list):
	seen = set()
	duplicates = set(item for item in user_list if item in seen or seen.add(item))
	return duplicates	
		
database = '/home/ct2767/Documents/coms_db/coms_db'
conn = create_connection(database)

with conn:
	fo = open("stats.txt", 'w')

	print("Getting table names...")
	table_list = return_table_names(conn)
	fo.writelines("========================================\n")

	for table in table_list:
		table_name = table
		fo.writelines("Class: " + table + "\n")
		fo.writelines("Mean: " + str(mean(conn)) + "\n")
	
		fo.writelines("Mode: " + str(mode(conn)) + "\n")

		fo.writelines("Standard deviation: " + str(std_dev(conn)) + "\n")
		fo.writelines("========================================\n")
	
	print("Stats completed.")
	fo.close()

	print("Writing users with entries in multiple tables to a separate file...")
	cur = conn.cursor()
	uni_list = []

	for table in table_list:
		cur.execute("SELECT UNI FROM '{tn}'".format(tn=table))
		result_list = [row[0] for row in cur.fetchall()]
		uni_list += result_list

	dup_list = find_duplicates(uni_list)

	fo = open("dupinfo.csv", 'w')
	fo.writelines("UNI,Class,Score,Prior Class,Score" + "\n")	

	for user in dup_list:
		info = []
		info.append(user)
		for table in table_list:
			cur.execute("SELECT * FROM '{tn}' WHERE UNI=?".format(tn=table), (str(user),))
			entry = cur.fetchone()
			if (entry is None or entry == []):
				pass			
			else:
				info.append(table)
				info.append(str(entry[-1]))
		fo.writelines(["%s," % item for item in info])
		fo.writelines("\n")
	
	print("Write complete.")
	fo.close()

conn.close()
