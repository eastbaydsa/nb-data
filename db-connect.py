#!/usr/bin/python2.7

import sys
import os
import urlparse
import psycopg2
import requests
import json

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

# nb access variables
nb_slug = "jefflee"
# read input for access token
input_args = sys.argv
if len(input_args) < 2:
	raise ValueError('provide NB access token as argument (ie. "./db.connect.py abc123")')
nb_access_token = input_args[1]
nb_request_headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
nb_request_url = "https://{0}.nationbuilder.com/api/v1/people?limit=100&__proto__=&access_token={1}".format(nb_slug, nb_access_token)

# hit NB for people data

# track all field keys from NB -- these will become the columns for SQL
unique_field_keys = []

def append_field(fields, key, value):
	key_str = str(key)
	# keep track of all unique field keys for creating the table later
	if key_str not in unique_field_keys:
		unique_field_keys.append(key_str)
	# filter out any empty values
	if value is None or value == '':
		return
	fields[key_str] = """'{0}'""".format(value)

# make request to NB for data
people_response_json = requests.get(nb_request_url, headers = nb_request_headers).json()

if 'results' not in people_response_json:
	raise IOError("got bad response from NB: {0}".format(people_response_json["message"]))

# iterate through results and track them
person_list = []
for person_json in people_response_json["results"]:
	person_fields = {}

	# iterate through fields for the person
	for field_key, field_value in person_json.items():

		# primary address fields are nested, so we have to iterate through the object
		if field_key == "primary_address" and field_value is not None:
			for address_key, address_value in field_value.items():
				# the key becomes 'primary_address_[field_name]'
				address_combined_key = "{0}_{1}".format(field_key, address_key)
				append_field(person_fields, address_combined_key, address_value)

		# tags show up as a list, so we have to concatenate the value.
		elif field_key == "tags" and field_value is not None:
			tags_value = ','.join("""{0}""".format(tag) for tag in field_value)
			append_field(person_fields, field_key, tags_value)

		else:
			append_field(person_fields, field_key, field_value)
	person_list.append(person_fields)

# ready the data for SQL input
sql_rows = []
for person in person_list:
	person_fields = []
	for field_key in unique_field_keys:
		if field_key not in person:
			person_fields.append('null')
		else:
			person_value = person[field_key]
			person_fields.append(person_value)
	sql_rows.append(','.join(person_fields))

# rows and columns for the INSERT statement
sql_insert_rows = ','.join("({0})".format(sql_row) for sql_row in sql_rows)			# wrap each row in parentheses
sql_insert_cols = ','.join(""""{0}\"""".format(key) for key in unique_field_keys)

# columns for the CREATE TABLE statement (all text for now)
sql_create_table_cols = ','.join(""""{0}" text""".format(key) for key in unique_field_keys)

# connect to postgres
conn = psycopg2.connect("dbname='db1' user='{0}' host='localhost' password='{1}'".format(user, password))
cur = conn.cursor()

# clean up old stuff
cur.execute("DROP SCHEMA IF EXISTS {0} CASCADE;".format(schema))

# create new schema	
cur.execute("CREATE SCHEMA IF NOT EXISTS {0} AUTHORIZATION {1};".format(schema, user))	

# create table
cur.execute("""CREATE TABLE IF NOT EXISTS {0} ({1}, PRIMARY KEY("email"));""".format(schema_table, sql_create_table_cols))

# do the insert
cur.execute("""INSERT INTO {0}({1}) VALUES {2}""".format(schema_table, sql_insert_cols, sql_insert_rows))

conn.commit()
cur.close
conn.close()

