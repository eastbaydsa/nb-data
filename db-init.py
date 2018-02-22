#!/usr/bin/python2.7

import sys
import os
import urlparse
import psycopg2

#urlparse.uses_netloc.append("postgres")
#url = urlparse.urlparse(os.environ["DATABASE_URL"])

#conn = psycopg2.connect(
#	database = url.path[1:],
#	user=url.username,
#	password=url.password,
#	host=url.hostname,
#	port=url.port
#)

user = "python"
password = "python"
schema = "nb"
table = "people"
schema_table = "{0}.{1}".format(schema, table)
columns_file = "export_columns.txt"
data_file = "fake_membership_data.csv"

# connect to postgres
conn = psycopg2.connect("dbname='db1' user='{0}' host='localhost' password='{1}'".format(user, password))
cur = conn.cursor()

# clean up old stuff
cur.execute("DROP SCHEMA IF EXISTS {0} CASCADE;".format(schema))

# create new schema	
cur.execute("CREATE SCHEMA IF NOT EXISTS {0} AUTHORIZATION {1};".format(schema, user))	

# create list of columns for table (all text for now)
columns_list = open(columns_file,"r").readline().split(" ")
columns_text = ','.join(""""{0}" text""".format(c.rstrip()) for c in columns_list)

# create table
cur.execute("""CREATE TABLE IF NOT EXISTS {0} ({1}, PRIMARY KEY("email"));""".format(schema_table, columns_text))

# construct insert query
f = open(data_file,"r")

data_columns = f.readline().rstrip()    											# first line has the columns
rows_txt = [row.strip() for row in f.readlines()]    								# get text for the rows
values_raw = []
for row in rows_txt:
	fields = ','.join("""'{0}'""".format(f.rstrip()) for f in row.split(","))		# wrap each field in quotes
	values_raw.append(fields)
values = ','.join("""({0})""".format(v) for v in values_raw) 						# wrap each row in parentheses

# do the insert
cur.execute("""INSERT INTO {0}({1}) VALUES {2}""".format(schema_table, data_columns, values))

conn.commit()
cur.close
conn.close()

