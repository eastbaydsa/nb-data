import pandas as pd
import numpy as np
import os
import itertools
from faker import Faker
from datetime import *

'''Below we import the data we've exported from Nation Builder and process it for further analysis
 The key steps are mering membership data and removing personal information'''

####### Declaring some functions
fake = Faker()

def email_scrub(x):

    if "@" in str(x):
        return fake.email()
    else:
        return np.nan

def notnull(s):
    return s.notnull().sum()


def import_data(file_name):
    os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Raw")
    all_data = pd.read_csv(file_name)

    return all_data

def generate_scub_cols(df):
    donation_columns = [x for x in df.columns if "donor" in x or "donation" in x]

    scrub_columns = ["website","facebook_username","twitter_login","meetup_id","primary_address1","primary_address2"
    ,"primary_address3","primary_submitted_address","address_address1","address_address2","address_address3"
    ,"billing_address1","address_submitted_address","billing_address1","billing_address2","billing_address3"
    ,"user_submitted_address1","user_submitted_address2","point_person_name_or_email",
                 "recruiter_name_or_email","is_donor"] + donation_columns

    return scrub_columns


#We can have multiple emails per unique person in NB. We don't know on which email the membership data will match
#so we have to merge on each

def member_merge(data_df,membership_df):

    '''

    :param data_df: NB data export
    :param membership_df: processed membership data that includes both email and AK_ID for unique ids
    :return:
    '''

    membership_df.rename(columns = {"join_date":"started_at","x_date":"expires on"}, inplace = True)

    members_with_email = membership_df.loc[membership_df["email_orig"].isnull() == False]
    members_with_id = membership_df.loc[membership_df["email_orig"].isnull() == True]


    data_df_copy = data_df.copy()

    #we want to ignore the members we track with Ids when we merge on emails
    all_data_minus_id = data_df_copy.loc[~data_df_copy["external_id"].isin(members_with_id["AK_id"].tolist())]

    merge_list = []

    #merging IDs - taking id-based membership data and export data that has an external id (want to avoid nans merging)
    id_merge = data_df_copy.loc[~all_data["external_id"].isin([np.nan])].merge(members_with_id[["AK_id","started_at","expires on"]], left_on = "external_id",right_on = "AK_id", how = "inner")
    merge_list.append(id_merge)
    print "id shape", id_merge.shape

    email_merge = all_data_minus_id.merge(members_with_email[["email_orig","started_at","expires on"]], left_on = "email",right_on = "email_orig", how = 'outer')
    print email_merge.shape

    export_with_members = pd.concat([email_merge,id_merge], axis = 0)
    export_with_members.drop_duplicates(inplace = True)

    #the code is reading the 2099 expiration dates as 1999
    export_with_members["started_at"] = export_with_members["started_at"].apply(lambda x: pd.to_datetime(x, errors = 'ignore',format = "%m/%d/%y"))
    export_with_members["expires on"] = export_with_members["expires on"].apply(lambda x: pd.to_datetime(x, errors = 'ignore',format = "%m/%d/%y"))


    return export_with_members


#scrubbing identifying information


def fake_data(df):

    '''

    :param df: nb export data frame
    :return:  nb export data frame with personal information turned fake
    '''
    df = df.copy()

    df["first_name"] = df["first_name"].apply(lambda x: fake.first_name())
    df["middle_name"] = df["middle_name"].apply(lambda x: fake.first_name())
    df["last_name"] = df["last_name"].apply(lambda x: fake.last_name())
    df[["email","email1","email2","email3"]] = all_data[["email","email1","email2","email3"]].applymap(email_scrub)
    df[["phone_number","work_phone_number","mobile_number","fax_number"]] = \
    df[["phone_number","work_phone_number","mobile_number","fax_number"]].applymap(lambda x: fake.phone_number())

    return df


#Converting the tags from a giant string to a list
def tag_split(df):

    '''

    :param df: nb export dataframe
    :return: nb export dataframe with tag_list turn from a string into a list
    '''

    df["tag_list"] = df["tag_list"].apply(lambda x: str(x).split(", "))
    df["tag_list"] = df["tag_list"].apply(lambda x: [y.lower() for y in x])

    #Dropping anyone who lacks a tag - should remove FB and twitter-only
    df["tag_list"] = df["tag_list"].apply(lambda x: np.nan if x == "['nan']" else x)
    all_data.dropna(subset = ["tag_list"], inplace = True)

    return df




def generate_unique_tags(df,export_date):

    '''

    :param df: dataframe of NB data
    :param export_date: date data was exported
    :return:
    '''

    unique_tags = set(list(itertools.chain.from_iterable(df["tag_list"])))
    #It will be easier to handle tags if everything is lowercase
    unique_tags =[x.lower() for x in unique_tags]
    unique_tag_series = pd.Series(data =unique_tags)

    return unique_tag_series





def file_out_put(df, file_name):

    export_date = datetime.strptime(file_name.split(".csv")[0][-10:],"%Y-%m-%d")
    export_date = export_date.strftime("%m%d%y")
    unique_tags = generate_unique_tags(df, export_date)


    process_file_name = "%s_processed.csv" % (file_name.split(".csv")[0])
    pickled_file_name = "%s_processed.pkl" % (file_name.split(".csv")[0])

    unique_tag_file_name  = "%s_%s.csv" %("unique_tags",export_date)
    unique_tag_pickle_name  = "%s_%s.pkl" %("unique_tags",export_date)


    os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Processed")
    df.to_csv(process_file_name)
    df.to_pickle(pickled_file_name)

    os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Unique Tags")
    unique_tags.to_csv(unique_tag_file_name)
    unique_tags.to_pickle(unique_tag_pickle_name)





os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Raw")
file_name = "nationbuilder-people-export-37-2018-02-26.csv"


os.chdir("/Users/seanmurphy/Documents/DSA/Member List/February 2018/feb_vs_jan")

membership_data = pd.read_csv("combined_membership.csv")


all_data = import_data(file_name)

all_data = member_merge(data_df = all_data,membership_df = membership_data)

all_data = tag_split(all_data)

scrub_columns = generate_scub_cols(all_data)

all_data[scrub_columns] = np.nan

print all_data
