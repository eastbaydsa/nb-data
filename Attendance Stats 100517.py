import pandas as pd
import numpy as np
import os
import itertools
from datetime import datetime, date
import itertools
from pandas.tseries.offsets import *
import matplotlib.pyplot as plt
from ggplot import *
####### Declaring some functions
def notnull(s):
    return s.notnull().sum()

today_append = datetime.strftime(date.today(),"%m%d%y")
#######################################################################################################################
def sample_size(s):
    return s.notnull().sum()
def gross_size(s):
    return s.shape[0]

########################################################################################################################
#Importing
########################################################################################################################
#Reading in the database and doing some prep
os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Processed")
all_data = pd.read_pickle("nationbuilder-people-export-32-2018-01-14_processed.pkl")

#Reading in unique tags
os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Unique Tags")
unique_tags = pd.read_csv("unique_tags_011418.csv", header = None)
########################################################################################################################
########################################################################################################################


########################################################################################################################
#Basic Tag Processing
########################################################################################################################

all_data["tag_list"] = all_data["tag_list"].apply(lambda x: np.nan if x == "['nan']" else x)
#Dropping anyone who lacks a tag - should remove FB and twitter-only
all_data.dropna(subset = ["tag_list"], inplace = True)
#This function creates a binary variable for the occurence of a tag
input_tags = ["national_member"]
def tag_to_column_binary(df):
    if len(input_tags) >= 1:
        t = input_tags[0]

        df[t] = df["tag_list"].apply(lambda x: True if t in x else False)

        del input_tags[0]
        tag_to_column_binary(df)
    return df
all_data = tag_to_column_binary(all_data)

unique_tags.rename(columns = {1:"tag_list"}, inplace = True)
unique_tag_list = unique_tags["tag_list"].dropna().tolist()

#Creating sub-lists of tags so we can create scores from them
meeting_tags = [x for x in unique_tag_list if "meeting_" in x]

canvass_tags = [x for x in unique_tag_list if "canvass_" in x ]
canvasser_tags = [x for x in unique_tag_list if "canvassers_" in x ]
canvasser_district_canvasses_tags = [x for x in unique_tag_list if "canvassers_" in x and "_d" in x]
print "district canvasser tags", canvasser_district_canvasses_tags
canvasser_big_canvasses_tags = [x for x in canvasser_tags if x not in canvasser_district_canvasses_tags]
social_tags = [x for x in unique_tag_list if "social_" in x]
tabling_tags = [x for x in unique_tag_list if "tabling_" in x]
memberlist_tags = [x for x in unique_tag_list if "member_list" in x]

non_member_event_tags = canvass_tags + tabling_tags + memberlist_tags
event_tags = [x for x in unique_tag_list if x[-2:] == "17" and x not in non_member_event_tags ]

########################################################################################################################
########################################################################################################################

########################################################################################################################
#Creating df of the event tags
########################################################################################################################
# will be able to search for event tags based on month ranges
event_dates = [ datetime.strptime(x[-6:],"%m%d%y") for x in event_tags ]
# event_dates = event_dates + [ datetime.strptime(x[-8:],"%m%d%Y") for x in event_tags if x[-4:] == "2017" ]

#Creating a dataframe of the events and their dates
event_dates_dict = {e: d for e,d in zip(event_dates,event_tags)}
event_date_df = pd.DataFrame.from_dict(event_dates_dict, orient = "index")
event_date_df.rename(columns = {0:"event_tag"}, inplace = True)

#Pulling useful date information from the datetime index
event_date_df["month"] = [x.month for x in event_date_df.index]
event_date_df["year"] = [x.year for x in event_date_df.index]
event_date_df["day"] = [x.day for x in event_date_df.index]
event_date_df["date"] = [x for x in event_date_df.index]

#Rolling each event data back to the 1st so we can easily merge it with
# dataframes that contain months of interest. Effectively, when we are interested if
# an event occured during a month, it's easy to just merge dataframes
#that remove 'day' from the datetime index
event_date_df["date_month_begin"] = event_date_df["date"].apply(lambda x: MonthBegin().rollback(x))

#aggregating events by month
events_by_year_month = event_date_df.groupby(by = ["date_month_begin"], as_index = False)["event_tag"].aggregate(sample_size)
events_by_year_month.rename(columns = {"event_tag":"event_count"},inplace = True)
events_by_year_month.sort_values(by = "date_month_begin", ascending = False, inplace = True)
events_by_year_month["cumulative_event_count"] = events_by_year_month["event_count"].cumsum()
print "event by year month"
print events_by_year_month

#this will allow us to determine the number of events a member could have attended
def events_after_date(start_date):

    #expecting a first-of-month rolled backward data
    events_after = events_by_year_month.loc[events_by_year_month["date_month_begin"] >= start_date,"event_count"].sum()

    return events_after

# Creating an alternative vesion that excludes canvasses from events (not excluding canvassers, only those canvassed)

non_canvass_event_df = event_date_df.loc[~event_date_df["event_tag"].isin(non_member_event_tags)]
print "test", non_canvass_event_df["event_tag"].tolist()
canvasser_event_df = event_date_df.loc[event_date_df["event_tag"].isin(canvasser_tags)]
district_canvasser_event_df = event_date_df.loc[event_date_df["event_tag"].isin(canvasser_district_canvasses_tags)]
big_canvasser_event_df = event_date_df.loc[event_date_df["event_tag"].isin(canvasser_big_canvasses_tags)]

# print "non canvass event df", non_canvass_event_df
# print "event df", event_date_df

########################################################################################################################
#Further Data Processing - adding new columns
########################################################################################################################
#THis function will find most recent event a person attended
#It takes the tag_list as the input
def most_recent(person_tag_list):

    #finding all events (excluding ppl who are canvassed)
    ignore_events = canvass_tags + memberlist_tags
    person_events = [x for x in person_tag_list if x[-2:] == "17" and x not in ignore_events]
    person_dates = [ datetime.strptime(x[-6:],"%m%d%y") for x in person_events if x[-4:] != "2017" ]
    # person_dates = person_dates + [ datetime.strptime(x[-8:],"%m%d%Y") for x in person_events if x[-4:] == "2017" ]

    if len(person_dates) > 0:
        most_recent = sorted(person_dates, key = lambda item:item, reverse = True)[0]

        return most_recent
    else:
        return np.nan

all_data["most_recent_event_date"] = all_data["tag_list"].apply(most_recent)
all_data["time_since_most_recent"] = all_data.apply(lambda x: datetime.today() - x["most_recent_event_date"], axis = 1)
all_data["day_since_most_recent"] = all_data["time_since_most_recent"].dt.days

#we may have some null start dates
all_data['test'] = all_data["started_at"].apply(lambda x: type(x))

#Creating useful date-based columns

all_data[["started_at","expires on"]] = all_data[["started_at","expires on"]].applymap(lambda x: pd.to_datetime(x, errors = 'ignore',format = "%m/%d/%y"))
all_data["start_month"] = all_data["started_at"].apply(lambda x: x.month)
all_data["start_year"] = all_data["started_at"].apply(lambda x: x.year)
all_data["start_month_rolled_back"] = all_data["started_at"].apply(lambda x: MonthBegin().rollback(x))

all_data["expiration_month"] = all_data["expires on"].apply(lambda x: x.month)
all_data["expiration_year"] = all_data["expires on"].apply(lambda x: x.year)

all_data["time_since_join"] = all_data.apply(lambda x: np.nan if pd.isnull(x["started_at"]) == True else  datetime.today() - x["started_at"], axis = 1)
all_data["days_since_join"] = all_data["time_since_join"].dt.days
all_data["months_since_join"] = all_data["time_since_join"].dt.days/30
all_data["months_since_join"] = all_data["months_since_join"].apply(lambda x: int(x) if np.isnan(x) == False else x)
all_data["expiration_year"] = all_data["expires on"].apply(lambda x: x.year)
all_data["expiration_month"] = all_data["expires on"].apply(lambda x: x.month)


#Finding where we have legit Timestamps
all_data["not_null_most_recent_event_date"] = all_data["most_recent_event_date"].notnull()
print "member check", all_data.loc[all_data["national_member"] == True].shape

members_by_start_year_month = all_data.groupby(by = "start_month_rolled_back", as_index = False )["first_name"].aggregate(gross_size)
members_by_start_year_month.rename(columns = {"first_name":"member_count"}, inplace = True)
########################################################################################################################

#Creating scores that measure the number of attendances within certain categories
all_data["meeting_score"] = all_data["tag_list"].apply(lambda x: len([ y for y in meeting_tags if y in x]))
all_data["canvass_score"] = all_data["tag_list"].apply(lambda x: len([ y for y in canvasser_tags if y in x]))
all_data["canvass_score"] = all_data["tag_list"].apply(lambda x: len([ y for y in canvasser_tags if y in x]))
all_data["district_canvass_score"] = all_data["tag_list"].apply(lambda x: len([ y for y in canvasser_district_canvasses_tags if y in x]))
all_data["big_canvass_score"] = all_data["tag_list"].apply(lambda x: len([ y for y in canvasser_big_canvasses_tags if y in x]))

all_data["event_score"] = all_data["tag_list"].apply(lambda x: len([ y for y in event_tags if y in x]))
all_data["social_score"] = all_data["tag_list"].apply(lambda x: len([ y for y in social_tags if y in x]))

########################################################################################################################
#This function creates a column of True-False values that correspond to the occurence of a tag
def tag_to_column_binary(df):
    if len(input_tags) >= 1:
        t = input_tags[0]
        # print t
        df[t] = df["tag_list"].apply(lambda x: True if t in x else False)

        del input_tags[0]
        tag_to_column_binary(df)
    return df
#We want to understand the number of unique attendees to our events
#We start by totaling gross attendees by month and then findign the unique number of people
#We divide the unique by the gross attendee count to get a ratio


#This function will tell us how many people have attended a meeting (of some type) x times between certain date range
#we use the event_df to find the tags within the date ranges we want and then identify the people
#First we find counts of consecutive attendance

def monthly_attendance_range(df,first_month,first_year,last_month,last_year):
    #datetime index of the time range

    start_date = datetime(first_year,first_month,1)
    end_date = datetime(last_year,last_month,1)

    date_range = pd.date_range(start = start_date, end = end_date, freq= "1MS" )
    #These are the events that fall in between the dates specified
    events_in_range = event_date_df.loc[event_date_df["date_month_begin"].isin(date_range),"event_tag"].tolist()

    #Calculating the number of events attended in the range specified

    #we need to name this range so we can reference it in a column
    range_name = "attendance_%s-%s" %(datetime.strftime(start_date, "%m%y"),datetime.strftime(end_date,"%m%y"))
    df[range_name] = df["tag_list"].apply(lambda x: True if len([e for e in events_in_range if e in x]) > 0 else False )
    return df

#This function takes in a month-year, the participant df and an df of events and their dates. it appends the participant
#df with two fields. THe first is a binary True or False to indicate whether that person attended something in that month-year
#The second is the total attendance records for that person in that month-year
def monthly_attendance(df,event_df,month,year):
    #datetime index of the month of inerest
    df_copy = df.copy()
    date = datetime(year,month,1)

    #These are the events that fall in between the dates specified
    events_in_month = event_df.loc[event_df["date_month_begin"] == date,"event_tag"].tolist()

    #Calculating the number of events attended in the range specified

    #we need to name this range so we can reference it in a column

    #Attendance Yes or No field name
    month_attendance = "attendance_%s" %(datetime.strftime(date, "%m%y"))
    #Total attendnace field name
    month_attendance_sum = "attendance_sum_%s" %(datetime.strftime(date, "%m%y"))

    #Attendance Yes or No
    df_copy[month_attendance] = df_copy["tag_list"].apply(lambda x: True if len([e for e in events_in_month if e in x]) > 0 else False )

    #Total Attendnace
    df_copy[month_attendance_sum] = df_copy["tag_list"].apply(lambda x: len([e for e in events_in_month if e in x])  )

    return df_copy[[month_attendance,month_attendance_sum]]
    # print events_in_month
    # print "monthly attendance output", df[range_name].sum()
    # print df[range_name]

print "check1", monthly_attendance(all_data,canvasser_event_df,9,2017)

#in this function, we are going to create binary attendance columns for each month in the range
# it will call upon the monthly_attendance function
def monthly_attendance_consecutive(df,event_df,first_month,first_year,last_month,last_year,min_attendance,operand,members_only):

    #Creating a copy of the participant df
    df_copy = df.copy()

    #First, we wanto create the month and year inputs that the monthly_attendance function can receive
    start_date = datetime(first_year,first_month,1)
    end_date = datetime(last_year,last_month,1)
    date_range = pd.date_range(start = start_date, end = end_date, freq= "1MS" )
    range_months = [x.month for x in date_range]
    range_years =[ x.year for x in date_range]
    range_name = "attendance_%s-%s" %(datetime.strftime(start_date, "%m%y"),datetime.strftime(end_date,"%m%y"))
    range_sum_name = "attendance_sum_%s-%s" %(datetime.strftime(start_date, "%m%y"),datetime.strftime(end_date,"%m%y"))

    attendance_names = ["attendance_%s" %(datetime.strftime(datetime(y,m,1), "%m%y")) for y,m in zip(range_years,range_months)]
    attendance_sum_names = ["attendance_sum_%s" %(datetime.strftime(datetime(y,m,1), "%m%y")) for y,m in zip(range_years,range_months)]

    #creating the monthly attendance columns
    monthly_df_list = [df_copy]
    for m,y in zip(range_months,range_years):

        #calculating attendance for each month
        monthly_attendance_df = monthly_attendance(df_copy,event_df,m,y)

        #adding each month-based calculation column to a list
        monthly_df_list.append(monthly_attendance_df)

    #combining df_copy with the monthly attendance dfs
    df_copy = pd.concat(monthly_df_list, axis = 1)

    #concatenating the monthly list, which include df as its base
    # df = pd.concat(monthly_df_list, axis = 1)

    #Creating columns to capture cumulative attendance

    #months in which they participcate
    df_copy[range_name] = df_copy[attendance_names].sum(axis = 1)

    #total number of times they participated
    df_copy[range_sum_name] = df_copy[attendance_sum_names].sum(axis = 1)

    # output_names = attendance_names + [range_name]
    # print df[output_names]
    if members_only == True:
        if operand == ">=":
            return df_copy.loc[(df_copy[range_name] >= min_attendance) & (df_copy["national_member"] == True)]
        elif operand == "=":
            return df_copy.loc[(df_copy[range_name] == min_attendance) & (df_copy["national_member"] == True)]
        elif operand == "":
            return df_copy.loc[all_data["national_member"] == True]
    else:
        if operand == ">=":
            return df_copy.loc[(df_copy[range_name] >= min_attendance) ]
        elif operand == "=":
            return df_copy.loc[(df_copy[range_name] == min_attendance) ]
        elif operand == "":
            return  df_copy

#For easy access, we create a function that returns the monthly and attendance range columns

def monthly_attendance_columns(first_month,first_year,last_month,last_year,range,sum):

    start_date = datetime(first_year,first_month,1)
    end_date = datetime(last_year,last_month,1)
    date_range = pd.date_range(start = start_date, end = end_date, freq= "1MS" )
    range_months = [x.month for x in date_range]
    range_years =[ x.year for x in date_range]

    range_name = "attendance_%s-%s" %(datetime.strftime(start_date, "%m%y"),datetime.strftime(end_date,"%m%y"))
    range_sum_name = "attendance_sum_%s-%s" %(datetime.strftime(start_date, "%m%y"),datetime.strftime(end_date,"%m%y"))

    attendance_sum_names = ["attendance_sum_%s" %(datetime.strftime(datetime(y,m,1), "%m%y")) for y,m in zip(range_years,range_months)]
    attendance_names = ["attendance_%s" %(datetime.strftime(datetime(y,m,1), "%m%y")) for y,m in zip(range_years,range_months)]

    if sum == True:
        attendance_name_output = attendance_sum_names
        range_output = range_sum_name
    else:
        attendance_name_output = attendance_names
        range_output = range_name

    if range == True:
        output_cols = [range_output] +  attendance_name_output
    else:
        output_cols = attendance_name_output
    return output_cols


#member attendance (overall counts by zip codes
#need to build in functionality for both sum of attendees (gross) and count of attendees (net)
def monthly_attendance_grouper(df,event_df,first_month,first_year,last_month,last_year,min_attendance,operand,members_only,range,grouper):

    monthly_data = monthly_attendance_consecutive(df,event_df,first_month,first_year,last_month,last_year,min_attendance,operand,members_only)

    #by default, we are looking for attendance sums, not the number of months in which someone attens
    #therefore, we take the monthly sum columns
    monthly_columns = monthly_attendance_columns(first_month,first_year,last_month, last_year,range,True)

    print monthly_data[monthly_columns].shape
    monthly_data_grouped = monthly_data.groupby(by = grouper,as_index = False)[monthly_columns].aggregate(np.sum)

    if range == True:
        #sorting by the range if available
        sort_col = monthly_columns[0]
    else:
        #sorting by the most recent month if not using the range
        sort_col = monthly_columns[-1]
    print monthly_data_grouped
    return monthly_data_grouped.sort_values(by = sort_col,ascending = False)

# print "monthly attendance by geog", monthly_attendance_geo(all,non_canvass_event_df,1,2017,12,2017,1,"",True,True,"address_zip")
########################################################################################################################
#Miscellaneous Analysis
########################################################################################################################
os.chdir(("/Users/seanmurphy/Documents/DSA/Export Analysis/Analyses/Attendance"))

attendance_by_zip = monthly_attendance_grouper(all_data,non_canvass_event_df,1,2017,12,2017,1,"",True,True,"address_zip")
attendance_by_zip.to_csv("attendance_by_zip.csv")

#attendance rates by start month - normalized by member count
attendance_by_join_date = monthly_attendance_grouper(all_data,non_canvass_event_df,1,2017,12,2017,1,"",True,True,["start_month_rolled_back"])
attendance_by_join_date["events_after_start"] = attendance_by_join_date["start_month_rolled_back"].apply(events_after_date)
attendance_by_join_date = attendance_by_join_date.merge(members_by_start_year_month,on = "start_month_rolled_back", how = "inner")
attendance_by_join_date["attendance_rate"] = attendance_by_join_date.apply(lambda x: 100 * x["attendance_sum_0117-1217"]/(x["member_count"] * x["events_after_start"]), axis = 1)


attendance_by_join_date[["start_month_rolled_back","events_after_start","member_count","attendance_rate"]].to_csv("attendance_by_join_date.csv")
#non attendees
# non_attendees = monthly_attendance_grouper(all_data,non_canvass_event_df,1,2017,12,2017,0,"=",True,True,"address_zip")
# non_attendees["address_zip"].to_csv("non_attendees_by_zip.csv")

print "check 3", [x for x in monthly_attendance_consecutive(all_data,non_canvass_event_df,8,2017,10,2017,3,">=",True).columns]
#The number of members who have come to at least one ther per month for the last 3  months
mv_1a = monthly_attendance_consecutive(all_data,non_canvass_event_df,9,2017,12,2017,3,">=",True).shape[0]
print "sep-dec member consecutive attendance", mv_1a
#The number of members who have come to at least one thing for the last six months
mv_1b = monthly_attendance_consecutive(all_data,non_canvass_event_df,7,2017,12,2017,6,">=",True).shape[0]
print "jul -dec member consecutive attendance", mv_1b
#The number of members who have come to at least on thing in december
mv_2 = monthly_attendance_consecutive(all_data,non_canvass_event_df,12,2017,12,2017,1,">=",True).shape[0]
print "members who've attended at least 1 thing in dec", mv_2
#The number of members who came to only one thing
mv_3_members = monthly_attendance_consecutive(all_data,non_canvass_event_df,1,2017,12,2017,1,"=",True).shape[0] #.loc[all_data["attendance_0917"] == True & all_data["attendance_0917"]
print "members who've attened only 1 thing in 2017", mv_3_members
#Now only those members who attended that one meeting in September

#the number of members who came to at least one thing
members_at_least_one_event = monthly_attendance_consecutive(all_data,non_canvass_event_df,1,2017,12,2017,1,">=",True).shape[0]
print "members at least on event", members_at_least_one_event

#The number of members who came to one thing
members_only_one_event =  monthly_attendance_consecutive(all_data,non_canvass_event_df,1,2017,12,2017,1,"=",True).shape[0]
print "members only one event", members_only_one_event

#The number of members/non-members who came to at least one thing
everyone_at_least_one_event =  monthly_attendance_consecutive(all_data,non_canvass_event_df,1,2017,12,2017,1,">=",False).shape[0]
print "eeryone at least one event", everyone_at_least_one_event

#The number of members/non-members who came to only one thing
everyone_one_event =  monthly_attendance_consecutive(all_data,non_canvass_event_df,1,2017,12,2017,1,"=",False).shape[0]
print "everyone one event", everyone_one_event

#Number of people who have canvassed in big and district canvasses
canvassers_district_and_big = all_data.loc[(all_data["big_canvass_score"] > 0) & (all_data["district_canvass_score"] > 0) & (all_data["national_member"] == True)]


#Number of people who have only canvassed in big canvasses
canvassers_big_only = all_data.loc[(all_data["big_canvass_score"] > 0) & (all_data["district_canvass_score"] == 0) & (all_data["national_member"] == True)]

#Number of people who have only canvassed in distrct canvasses
canvassers_district_only = all_data.loc[(all_data["big_canvass_score"] == 0) & (all_data["district_canvass_score"] > 0) & (all_data["national_member"] == True)]

print "canvass attendance", canvassers_district_and_big.shape[0], canvassers_big_only.shape[0], canvassers_district_only.shape[0]

print "check2", monthly_attendance_consecutive(all_data,non_canvass_event_df,9,2017,12,2017,3,">=",True).shape

print "MV data", mv_1a,mv_1b, mv_2, mv_3_members,  members_only_one_event, members_at_least_one_event, everyone_at_least_one_event, everyone_one_event
########################################################################################################################
########################################################################################################################

#Net vs gross attendance
# sept_oct =  monthly_attendance_consecutive(all_data,non_canvass_event_df,9,2017,10,2017,1,"",True)
# sept_gross = sept_oct["attendance_sum_0917"].sum()
# sept_net = sept_oct["attendance_0917"].sum()
def net_gross(df,event_df,first_month,first_year,last_month,last_year,members_only):

    attendance = monthly_attendance_consecutive(df,event_df,first_month,first_year,last_month,last_year,1,"",members_only)

    start_date = datetime(first_year,first_month,1)
    end_date = datetime(last_year,last_month,1)
    date_range = pd.date_range(start = start_date, end = end_date, freq= "1MS" )
    range_months = [x.month for x in date_range]
    range_years =[ x.year for x in date_range]

    #Binary attendance
    attendance_names = ["attendance_%s" %(datetime.strftime(datetime(y,m,1), "%m%y")) for y,m in zip(range_years,range_months)]
    #Total attendnace (Here is where we measure multuple attendances)
    attendance_sum_names = ["attendance_sum_%s" %(datetime.strftime(datetime(y,m,1), "%m%y")) for y,m in zip(range_years,range_months)]
    time_index = [datetime(y,m,1) for y,m in zip(range_years,range_months)]
    # print time_index
    #
    # print "check 4"
    # print attendance[attendance_names].sum()
    # print attendance[attendance_sum_names].sum()
    attendance_sum_list = attendance[attendance_sum_names].sum().tolist()
    attendance_unique_list = attendance[attendance_names].sum().tolist()
    attendance_df = pd.DataFrame(data = attendance_unique_list, index = time_index, columns = ["Unique Attendees"])
    attendance_df["Total Attendees"] = attendance_sum_list
    attendance_df["Percent Unique"] = attendance_df["Unique Attendees"] / attendance_df["Total Attendees"]


    print attendance_df
    # print [x for x in attendance.columns]

net_gross(all_data,non_canvass_event_df,1,2017,12,2017,True)

# MV's request'
# 1- number of people who have come to at least one thing per month for the last three months, and the last six months.
# 2 - total number of people who have come to at least one thing in September.
# 3 - total number of people who come to at least one thing in September who had never attended anything before.

#######################################################################################################
#Plotting


#The following graphs will only be produced for members

#Most Recent Attendance plot
member_attendance = all_data.loc[(all_data["national_member"] == True)]# & (all_data["not_null_most_recent_event_date"] == True)]

recent_attendance_plot = ggplot(aes(x = "day_since_most_recent"), all_data.loc[(all_data["national_member"] == True) & (all_data["not_null_most_recent_event_date"] == True)])
recent_attendance_hist = recent_attendance_plot + geom_histogram()
print "member attendance check",member_attendance.shape, member_attendance[["meeting_score","day_since_most_recent"]]
# print all_data[["time_since_most_recent","day_since_most_recent"]]


#Scored-based plots

meeting_plot = ggplot(aes(x = "meeting_score"), all_data.loc[all_data["national_member"] == True])
meeting_hist = meeting_plot + geom_histogram()

social_plot = ggplot(aes(x = "social_score"), all_data.loc[all_data["national_member"] == True ])
social_hist = social_plot + geom_histogram()

canvass_plot = ggplot(aes(x = "canvass_score"), all_data.loc[all_data["national_member"] == True ])
canvass_bar = canvass_plot + geom_bar()
print "total canvassers", all_data["canvass_score"].sum()

#boxplot of days since last attendance by meeting score
def attendance_by_event_type_box(member_attendance,event_score,facet,facet_var):

    if facet == False:

        attendance_by_meeting_grouped = member_attendance.groupby(by = event_score)["first_name"].aggregate(sample_size)
        x_breaks = [x for x in attendance_by_meeting_grouped.index]
        sample_sizes = attendance_by_meeting_grouped.tolist()
        x_labels = ["%s\nn=%s" %(x,y) for x,y in zip(x_breaks,sample_sizes)]

        print "pepper", x_labels, sample_sizes
        attendance_by_event_box = ggplot(member_attendance, aes(x = event_score,y = "day_since_most_recent")) + geom_boxplot() \
        + scale_x_discrete(breaks = x_breaks, labels = x_labels )

    else:
        #need to repeat calculations because facet vars can't be Nan
        member_attendance.dropna(subset = [facet_var],inplace = True)
        attendance_by_meeting_grouped = member_attendance.groupby(by = event_score)["first_name"].aggregate(sample_size)
        x_breaks = [x for x in attendance_by_meeting_grouped.index]
        sample_sizes = attendance_by_meeting_grouped.tolist()
        x_labels = ["%s\nn=%s" %(x,y) for x,y in zip(x_breaks,sample_sizes)]

        attendance_by_event_box = ggplot(member_attendance, aes(x = event_score,y = "day_since_most_recent")) + geom_boxplot() \
        + scale_x_discrete(breaks = x_breaks, labels = x_labels ) + facet_wrap(x = facet_var)


    print attendance_by_event_box

# print 'box check', attendance_by_event_type_box(member_attendance,"event_score",True,"expiration_year")
# def attendance_by_event_type_and_expiration)
member_attendance_2018_expiration = all_data.loc[(all_data["national_member"] == True) & (all_data["expiration_year"] == 2018)]
member_attendance_2018_expiration_grouped = member_attendance_2018_expiration.groupby(by = "event_score")["first_name"].aggregate(sample_size)

member_attendance_2018_expiration_box = ggplot(member_attendance_2018_expiration, aes(x = "social_score")) + geom_bar() + facet_wrap(x = "expiration_month")
# print member_attendance_2018_expiration_box
#Graphs to produce
# Histograms of activity scores (done_
# Histogram of time since last event (done, could^^ improve on x-axis. right now it's just days since last evnet)
# Box plot of time since last meeting by score (done, could split into two groups (meeting score < and > 4
# Net to gross by month

#Boxplot of total attendance in 2017 by months since joining
member_attendance_2017 = monthly_attendance_consecutive(all_data,non_canvass_event_df,1,2017,12,2017,1,"",True)

#Non member attendance (excludes those canvassed, fb, twitter) - only ppl who've provided emails
non_member_attendance_2017 = monthly_attendance_consecutive(all_data.dropna(subset = ["email"]),non_canvass_event_df,1,2017,12,2017,1,"",False)

member_attendance_meeting_by_join = ggplot(member_attendance_2017.loc[member_attendance_2017["months_since_join"] <=12], aes(x = "attendance_0117-1217")) + geom_bar() + facet_wrap("months_since_join")
non_member_attendance_meeting_by_join = ggplot(non_member_attendance_2017, aes(x = "attendance_0117-1217")) + geom_bar()

# member_attendance_2017.loc[member_attendance_2017["attendance_0117-0917"] > 0].shape

#who are the people with
