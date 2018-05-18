import pandas as pd
import numpy as np
import os
import pdb
import itertools
from datetime import datetime, date
import itertools
from pandas.tseries.offsets import *
import matplotlib.pyplot as plt

####### Declaring some functions
def notnull(s):
    return s.notnull().sum()


today_append = datetime.strftime(date.today(),"%m%d%y")
#######################################################################################################################
def sample_size(s):
    return s.notnull().sum()
def gross_size(s):
    return s.shape[0]
def tag_split(df):

    '''

    :param df: data frame of nb data
    :return:  data frame of nb with tags turned from a long string to a list of strings
    '''

    df["tag_list"] = df["tag_list"].apply(lambda x: str(x).split(", "))
    df["tag_list"] = df["tag_list"].apply(lambda x: [y.lower() for y in x])

    #Dropping anyone who lacks a tag - should remove FB and twitter-only
    df["tag_list"] = df["tag_list"].apply(lambda x: np.nan if x == "['nan']" else x)
    df.dropna(subset = ["tag_list"], inplace = True)

    return df

########################################################################################################################
#Importing
########################################################################################################################
#Reading in the database and doing some prep
os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Processed")
all_data = pd.read_pickle("nationbuilder-people-export-37-2018-02-26_processed.pkl")




#Reading in unique tags
os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Exports/Unique Tags")
unique_tags = pd.read_csv("unique_tags_022618.csv", header = None)
unique_tags.rename(columns = {1:"tag_list"}, inplace = True)
unique_tag_list = unique_tags["tag_list"].dropna().tolist()

########################################################################################################################
########################################################################################################################


########################################################################################################################
#Basic Tag Processing
########################################################################################################################


#Creating sub-lists of tags so we can create scores from them
meeting_tags = [x for x in unique_tag_list if "meeting_" in x]
phonebank_tags = [x for x in unique_tag_list if "phone" in x]
canvass_tags = [x for x in unique_tag_list if "canvass_" in x ]
canvasser_tags = [x for x in unique_tag_list if "canvassers_" in x ]
canvasser_district_canvasses_tags = [x for x in unique_tag_list if "canvassers_" in x and "_d" in x]
canvasser_big_canvasses_tags = [x for x in canvasser_tags if x not in canvasser_district_canvasses_tags]

training_tags = [x for x in unique_tag_list if "training_" in x]
education_tags = [x for x in unique_tag_list if "education_" in x]

social_tags = [x for x in unique_tag_list if "social_" in x]

tabling_tags = [x for x in unique_tag_list if "tabling_" in x]
memberlist_tags = [x for x in unique_tag_list if "member_list" in x]


non_member_event_tags = canvass_tags + tabling_tags + memberlist_tags + ["signup_newsletter_100117"]

event_tags = [x for x in unique_tag_list if x[-2:] == "17" and x not in non_member_event_tags ]
other_event_tags = [x for x in event_tags if x not in list(itertools.chain.from_iterable([social_tags,education_tags,training_tags,canvasser_tags, meeting_tags])) ]

print 'event tags !!!', sorted(other_event_tags)


########################################################################################################################
########################################################################################################################

########################################################################################################################
#Creating df of the event tags
########################################################################################################################


def create_event_df(event_tag_list):
    '''

    :param event_tag_list: list of tags of interest
    :return: a timeseries of events
    The dataframe will be useful for analyzing events by time
    '''

    event_dates = [ datetime.strptime(x[-6:],"%m%d%y") for x in event_tags ]

    #Creating a dataframe of the events and their dates
    event_dates_dict = {e: d for e,d in zip(event_dates,event_tags)}
    event_date_df = pd.DataFrame.from_dict(event_dates_dict, orient = "index")
    event_date_df.rename(columns = {0:"event_tag"}, inplace = True)

    #Pulling useful date information from the datetime index
    event_date_df["month"] = [x.month for x in event_date_df.index]
    event_date_df["year"] = [x.year for x in event_date_df.index]
    event_date_df["day"] = [x.day for x in event_date_df.index]
    event_date_df["date"] = [x for x in event_date_df.index]

    event_date_df["date_month_begin"] = event_date_df["date"].apply(lambda x: MonthBegin().rollback(x))

    return event_date_df


def count_events_by_time(event_date_df, by_year_month, cumulative):
    '''

    :param event_date_df: time series of events
    :param by_year_month: True or False. True means we count by year_month, False means we count by year
    :param cumulative: True or Fale. If True, return number of events that have cumulatively occurred through a given
    time increment. If False, just return the count of events in each time increment
    :return: count of events in the selected time increment
    '''
    if by_year_month == True:
        time_var = "date_month_begin"
    else:
        time_var = "year"

    #counting the events by time increment
    events_by_time = event_date_df.groupby(by = time_var, as_index = False)["event_tag"].aggregate(sample_size)
    events_by_time.rename(columns = {"event_tag":"event_count"},inplace = True)

    if cumulative == False:

        return events_by_time
    else:

        events_by_time.sort_values(by = time_var, ascending = False, inplace = True)
        events_by_time["cumulative_event_count"] = events_by_time["event_count"].cumsum()

        return events_by_time


#this will allow us to determine the number of events a member could have attended
def events_after_date(event_date_df,start_date):

    '''

    :param event_date_df: time series of events
    :param start_date: date after which we want to find events
    :return: time series of events that occurred after a particular date
    '''

    #expecting a first-of-month rolled backward data

    events_after = event_date_df.loc[event_date_df["date_month_begin"] >= start_date,"event_count"].sum()

    return events_after


# Creating an alternative vesion that excludes canvasses from events (not excluding canvassers, only those canvassed)

non_canvass_event_df = event_date_df.loc[~event_date_df["event_tag"].isin(non_member_event_tags)]


########################################################################################################################
#Further Data Processing - adding new columns
########################################################################################################################
#THis function will find most recent event a person attended
#It takes the tag_list as the input
def most_recent(person_tag_list):

    '''
    THis function identies the most recent event a person has attended
    :param person_tag_list: a person's list of tags
    :param candidate_tags: all possible event tags - used to exclude non event tags
    :return: the date of the most recent event tag
    '''

    # #finding all events (excluding ppl who are canvassed)
    ignore_events = canvass_tags + memberlist_tags
    person_events = [x for x in person_tag_list if x[-2:] in ["17","18"] and x not in ignore_events]

    person_dates = [ datetime.strptime(x[-6:],"%m%d%y") for x in person_events if x[-4:] not in ["2017","2018"] ]

    # person_dates = person_dates + [ datetime.strptime(x[-8:],"%m%d%Y") for x in person_events if x[-4:] == "2017" ]

    if len(person_dates) > 0:
        most_recent = sorted(person_dates, key = lambda item:item, reverse = True)[0]

        return most_recent
    else:
        return np.nan

########################################################################################################################

#Creating scores that measure the number of attendances within certain categories
def attendance_scores(df):
    # df = df.copy()
    df["meeting_score"] = df["tag_list"].apply(lambda x: len([ y for y in meeting_tags if y in x]))

    df["phonebank_score"] = df["tag_list"].apply(lambda x: len([ y for y in phonebank_tags if y in x]))
    df["canvass_score"] = df["tag_list"].apply(lambda x: len([ y for y in canvasser_tags if y in x]))
    df["canvass_score"] = df["tag_list"].apply(lambda x: len([ y for y in canvasser_tags if y in x]))
    df["district_canvass_score"] = df["tag_list"].apply(lambda x: len([ y for y in canvasser_district_canvasses_tags if y in x]))
    df["big_canvass_score"] = df["tag_list"].apply(lambda x: len([ y for y in canvasser_big_canvasses_tags if y in x]))

    df["education_score"] = df["tag_list"].apply(lambda x: len([ y for y in education_tags if y in x]))

    df["event_score"] = df["tag_list"].apply(lambda x: len([ y for y in event_tags if y in x]))
    df["social_event_score"] = df["tag_list"].apply(lambda x: len([ y for y in social_tags if y in x]))


    df["paper_member"] = df.apply(lambda x : all([x["social_event_score"] == 0,x["meeting_score"] == 0, x["canvass_score"] == 0, x["education_score"] ==0, x["phonebank_score"] == 0]), axis = 1)
    # all_data["paper_member"] = all_data.apply(lambda x : True if x["event_score"] == True else False, axis = 1)


    return df



# os.chdir("/Users/seanmurphy/Documents/DSA/Export Analysis/Analyses/Attendance/scores")
# paper_members.to_csv("paper_members.csv")


########################################################################################################################
#Creating useful date-based columns
'''Creating some useful date-based columns'''
def create_time_fields(df):
    '''

    :param df: dataframe of NB data
    :return: dataframe of NB data with useful time fields
    '''

    #Turning important fields from string into datetime
    df[["started_at","expires on"]] = df[["started_at","expires on"]].applymap(lambda x: pd.to_datetime(x, errors = 'ignore',format = "%m/%d/%y"))

    #creating some useful values
    df["start_month"] = df["started_at"].apply(lambda x: x.month)
    df["start_year"] = df["started_at"].apply(lambda x: x.year)
    df["start_month_rolled_back"] = df["started_at"].apply(lambda x: MonthBegin().rollback(x))

    df["expiration_month"] = df["expires on"].apply(lambda x: x.month)
    df["expiration_year"] = df["expires on"].apply(lambda x: x.year)



    df["time_since_join"] = df.apply(lambda x: np.nan if pd.isnull(x["started_at"]) == True else  datetime.today() - x["started_at"], axis = 1)
    df["days_since_join"] = df["time_since_join"].dt.days
    df["months_since_join"] = df["time_since_join"].dt.days/30
    df["months_since_join"] = df["months_since_join"].apply(lambda x: int(x) if np.isnan(x) == False else x)


    #does this need to remove non event tags?
    df["most_recent_event_date"] = df["tag_list"].apply(most_recent)

    df["time_since_most_recent"] = df.apply(lambda x: datetime.today() - x["most_recent_event_date"], axis = 1)
    df["day_since_most_recent"] = df["time_since_most_recent"].dt.days


    #Finding where we have legit Timestamps
    df["not_null_most_recent_event_date"] = df["most_recent_event_date"].notnull()


    return df


########################################################################################################################


'''This function creates a column of True-False values that correspond to the occurence of a tag'''
def tag_to_column_binary(df,tags_of_interest):

    '''
    :param df: dataframe of NB data
    :param tags_of_interest: tags that we are turning into binary columns
    :return: data frame with new columns indicating presence of certain tags
    '''

    if len(tags_of_interest) >= 1:
        t = tags_of_interest[0]

        df[t] = df["tag_list"].apply(lambda x: True if t in x else False)

        del tags_of_interest[0]
        tag_to_column_binary(df,tags_of_interest)
    return df
'''We want to understand the number of unique attendees to our events
We start by totaling gross attendees by month and then findign the unique number of people
We divide the unique by the gross attendee count to get a ratio'''


'''This function will tell us how many people have attended a meeting (of some type) x times between certain date range
we use the event_df to find the tags within the date ranges we want and then identify the people
First we find counts of consecutive attendance'''

def monthly_attendance_range(df,first_month,first_year,last_month,last_year):

    '''

    :param df: dataframe of NB data
    :param first_month:
    :param first_year:
    :param last_month:
    :param last_year:
    :return:
    '''
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

'''This function takes in a month-year, the participant df and an df of events and their dates. it appends the participant
df with two fields. THe first is a binary True or False to indicate whether that person attended something in that month-year
The second is the total attendance records for that person in that month-year'''

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

'''in this function, we are going to create binary attendance columns for each month in the range
it will call upon the monthly_attendance function'''
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
            return df_copy.loc[df_copy["national_member"] == True]
    else:
        if operand == ">=":
            return df_copy.loc[(df_copy[range_name] >= min_attendance) ]
        elif operand == "=":
            return df_copy.loc[(df_copy[range_name] == min_attendance) ]
        elif operand == "":
            return  df_copy

''''For easy access, we create a function that returns the monthly and attendance range columns'''

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


'''member attendance (overall counts by zip codes
 need to build in functionality for both sum of attendees (gross) and count of attendees (net)'''
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

########################################################################################################################
########################################################################################################################
########################################################################################################################
#Miscellaneous Analysis
########################################################################################################################


all_data = tag_to_column_binary(all_data,["national_member"])
print "we have %s members (including recent expirations)" % str(all_data.loc[all_data["national_member"] == True].shape[0])


all_data = create_time_fields(all_data)


members_by_start_year_month = all_data.groupby(by = "start_month_rolled_back", as_index = False )["first_name"].aggregate(gross_size)

members_by_start_year_month.rename(columns = {"first_name":"member_count"}, inplace = True)


os.chdir(("/Users/seanmurphy/Documents/DSA/Export Analysis/Analyses/Attendance"))

event_date_df = create_event_df(event_tags)



#aggregating events by month
events_by_year_month = count_events_by_time(event_date_df = event_date_df,by_year_month= True, cumulative= True)



all_data = attendance_scores(all_data)
paper_members = all_data.loc[(all_data["paper_member"] == True) & (all_data["national_member"] == True), ["email","paper_member","tag_list","social_event_score","meeting_score","canvass_score"]].dropna(subset = ["email"])
print "meeting score output", paper_members

attendance_by_zip = monthly_attendance_grouper(all_data,non_canvass_event_df,1,2017,12,2017,1,"",True,True,"address_zip")


