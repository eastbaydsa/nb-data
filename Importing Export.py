import pandas as pd
import numpy as np
import os
import itertools
from faker import Faker
from datetime import *


####### Declaring some functions
fake = Faker()

def email_scrub(x):

    if "@" in str(x):
        return fake.email()
    else:
        return np.nan

def notnull(s):
    return s.notnull().sum()

today_append = date.strftime(date.today(),"%m%d%y")

################################################################################################################
#Importing the Export Structure
os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Raw")
file_name = "nationbuilder-people-export-33-2018-01-20.csv"
process_file_name = "%s_processed.csv" % (file_name.split(".csv")[0])
pickled_file_name = "%s_processed.pkl" % (file_name.split(".csv")[0])
print process_file_name
all_data = pd.read_csv(file_name)

export_date = datetime.strptime(file_name.split(".csv")[0][-10:],"%Y-%m-%d")
export_date = export_date.strftime("%m%d%y")

#Import membership data
os.chdir("/Users/seanmurphy/Documents/DSA/Member List/December 2017/Import")

members_with_email = pd.read_csv("active_members_db_with_email_123117.csv" ,skiprows =1)
# members_with_email[["started_at","expires on"]] = members_with_email[["started_at","expires on"]].apply(lambda x: pd.to_datetime(x, format = "%m/%d/%y"))


members_with_id = pd.read_csv("active_members_db_no_email_123117.csv", skiprows = 1)

#We can have multiple emails per unique person in NB. We don't know on which email the membership data will match
#so we have to merge on each
print all_data.shape
def member_merge(all_data,members_with_email,members_with_id):
    all_data_copy = all_data.copy()

    #we want to ignore the members we track with Ids when we merge on emails
    all_data_minus_id = all_data_copy.loc[~all_data_copy["external_id"].isin(members_with_id["external_id"].tolist())]

    merge_list = []

    #merging IDs - taking id-based membership data and export data that has an external id (want to avoid nans merging)
    id_merge = all_data_copy.loc[~all_data["external_id"].isin([np.nan])].merge(members_with_id[["external_id","started_at","expires on"]], on = "external_id", how = "inner")
    merge_list.append(id_merge)
    print "id shape", id_merge.shape

    email_merge = all_data_minus_id.merge(members_with_email[["email Address","started_at","expires on"]], left_on = "email",right_on = "email Address", how = 'outer')


    export_with_members = pd.concat([email_merge,id_merge], axis = 0)
    export_with_members.drop_duplicates(inplace = True)

    #the code is reading the 2099 expiration dates as 1999
    export_with_members["started_at"] = export_with_members["started_at"].apply(lambda x: pd.to_datetime(x, errors = 'ignore',format = "%m/%d/%y"))
    export_with_members["expires on"] = export_with_members["expires on"].apply(lambda x: pd.to_datetime(x, errors = 'ignore',format = "%m/%d/%y"))


    return export_with_members



all_data = member_merge(all_data,members_with_email,members_with_id)


########################################################################################################################
#scrubbing identifying information


donation_columns = [x for x in all_data.columns if "donor" in x or "donation" in x]
scrub_columns = ["website","facebook_username","twitter_login","meetup_id","primary_address1","primary_address2"
    ,"primary_address3","primary_submitted_address","address_address1","address_address2","address_address3"
    ,"billing_address1","address_submitted_address","billing_address1","billing_address2","billing_address3"
    ,"user_submitted_address1","user_submitted_address2","point_person_name_or_email",
                 "recruiter_name_or_email","is_donor"] + donation_columns

def fake_date(df):

    df = df.copy()

    df["first_name"] = df["first_name"].apply(lambda x: fake.first_name())
    df["middle_name"] = df["middle_name"].apply(lambda x: fake.first_name())
    df["last_name"] = df["last_name"].apply(lambda x: fake.last_name())
    df[["email","email1","email2","email3"]] = all_data[["email","email1","email2","email3"]].applymap(email_scrub)
    df[["phone_number","work_phone_number","mobile_number","fax_number"]] = \
    df[["phone_number","work_phone_number","mobile_number","fax_number"]].applymap(lambda x: fake.phone_number())

    return df


#Converting the tags from a giant string to a list
all_data["tag_list"] = all_data["tag_list"].apply(lambda x: str(x).split(", "))
all_data["tag_list"] = all_data["tag_list"].apply(lambda x: [y.lower() for y in x])


all_data[scrub_columns] = np.nan

#########################################################################################################


os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Processed")
all_data.to_csv(process_file_name)
all_data.to_pickle(pickled_file_name)
#Redirecting towards the output folder
os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Counts by Zip")
################################################################################################################
#Converting the tags from strings to comma separate lists


#################################################################################################################
#Setting up Tags
os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Unique Tags")

unique_tags = set(list(itertools.chain.from_iterable(all_data["tag_list"])))
#It will be easier to handle tags if everything is lowercase
unique_tags =[x.lower() for x in unique_tags]
unique_tag_series = pd.Series(data =unique_tags)

unique_tag_file = "%s_%s.csv" %("unique_tags",export_date)
unique_tag_series.to_csv(unique_tag_file)
##################################################################################################
all_data["test"] = all_data["started_at"].apply(lambda x: type(x))
# all_data['test'] = all_data["started_at"].apply(lambda x: np.nan if x == 'NaT' else datetime.strptime(x,"%m/%d/%y"))
print all_data[["started_at","expires on","test"]]