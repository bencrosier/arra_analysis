import pandas as pd
import numpy as np
import datetime, pytz
from sna_metrics import add_sna_metrics
from one_hot_encoding import convert_columns
#import os.path.getmtime as check_time
import psycopg2
from pkg_resources import resource_string

def db_string():
    resource_package = __name__  # Could be any module/package name
    resource_path = '/'+'.db'  # Do not use os.path.join(), see below
    return resource_string(resource_package, resource_path).replace('\n','')

def get_connection():
    conn = psycopg2.connect(db_string())
    return conn
 
def show_tables():
    conn = get_connection()
    
    table_list = pd.read_sql("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE 'main_%'
            """, conn)

    col_list = pd.read_sql("""
                SELECT column_name, table_name 
                FROM information_schema.columns
                WHERE table_name LIKE 'main_%'
                """, conn)
    
    # Generate a dictionary containing all column names
    DF_dict = {}
    for name in list(table_list['table_name']):
        DF_dict[name] = list(col_list[col_list['table_name'] == name]['column_name'])
    
    # Fill it up so all lists are equal length
    max_length = max([len(f) for f in DF_dict.values()])
    for key, value in DF_dict.items():
        DF_dict[key] = DF_dict[key] + ['-----'] * (max_length - len(DF_dict[key]))
    
    # Create a DataFrame
    all_columns = pd.DataFrame(DF_dict)
    
    conn.close()
    return all_columns

def media_data():
    conn = get_connection()
    sql_query = """
            SELECT "userId", "captionText", "createdTime" 
            FROM main_instagrammedia
            """
    tt = pd.read_sql(sql_query, conn)
    conn.close()
    return tt

def arradata_raw():
    conn = get_connection()
    sql_query = """
        SELECT 
            distinct main_user."userId", 
            age as survey_age, 
            gender as survey_gender, 
            race as survey_race, 
            height as survey_height, 
            weight as survey_weight, 
            drinks as survey_drinks, 
            tobacco as survey_tobacco, 
            illegal_drugs as survey_illegal_drugs, 
            prescription_drugs as survey_prescription_drugs, 
            breakup as survey_breakup, 
            creativity as survey_creativity, 
            death_loved as survey_death_loved, 
            food as survey_food, 
            happy as survey_happy, 
            major_friendship as survey_major_friendship, 
            narcissist as survey_narcissist, 
            other_loss as survey_other_loss,
            phq1 as survey_phq1,
            phq2 as survey_phq2,
            phq3 as survey_phq3,
            phq4 as survey_phq4,
            phq5 as survey_phq5,
            phq6 as survey_phq6,
            phq7 as survey_phq7,
            phq8 as survey_phq8,
               
            media, 
            likes, 
            comments, 
            follows, 
            followed_by
        FROM main_user
        JOIN main_survey 
        ON main_user.survey_id = main_survey.id 
        LEFT JOIN (
                   SELECT 
                       COALESCE( SUM(likes), 0 ) AS likes, 
                       COALESCE( sum(comments), 0 ) AS comments, 
                       survey_id AS id 
                   FROM main_instagrammedia 
                   GROUP BY survey_id
                  ) media_totals 
        ON main_survey.id = media_totals.id
        ;"""
    arradata = pd.read_sql(sql_query, conn)
    conn.close()
    return arradata

def data(refresh = False):
    
    if refresh == False:
        try:
            return pd.read_pickle('arradatafile')
        except IOError:
            print "no arradata file, getting data;"
    
    arradata = arradata_raw()
    
    #reset index
    arradata.index = list(arradata['userId'])
    arradata = arradata.drop('userId', axis = 1)
    
    arradata['mean_likes'] = arradata['likes'] / (arradata['media'] + 1)
    arradata['mean_comments'] = arradata['comments'] / (arradata['media'] + 1)
    arradata['follow_ratio'] = arradata['followed_by'] / (arradata['follows'] + 1)
    arradata['magnetism'] = arradata['mean_likes'] * arradata['mean_comments'] * arradata['follow_ratio']
    arradata['survey_bmi'] = arradata['survey_weight'] / ((arradata['survey_height'] / 100) **2)
    
    arradata['log_follow_ratio'] = np.log(arradata['follow_ratio'] + 1)
    arradata['log_mean_activity'] = np.log(arradata['mean_likes'] * arradata['mean_comments'] + 1)
    arradata['log_magnetism'] = np.log(arradata['magnetism'] + 1)
    
    tt = media_data()
    
    tt['caption_length'] = tt['captionText'].str.len().fillna(0)
    arradata['mean_caption_length'] = tt['caption_length'].groupby(tt['userId']).mean()
    arradata['mean_caption_length'] = arradata['mean_caption_length'].fillna(0)
    
    utc_tz = pytz.timezone("UTC")
    cnt_tz = pytz.timezone("US/Central")
    def get_timeofday(timestamp):
        utc_time = datetime.datetime.utcfromtimestamp(timestamp)
        cnt_time = utc_tz.localize(utc_time).astimezone(cnt_tz)
        hour = cnt_time.hour
        return hour
    
    def is_weekday(timestamp):
        utc_time = datetime.datetime.utcfromtimestamp(timestamp)
        cnt_time = utc_tz.localize(utc_time).astimezone(cnt_tz)
        wday = cnt_time.weekday()
        if wday <= 4: weekday = True
        else: weekday = False
        return weekday
    
    tt['is_weekday'] = tt[['createdTime']].applymap(is_weekday)['createdTime']
    tt['time_of_day'] = tt[['createdTime']].applymap(get_timeofday)['createdTime']
    
    week_av = tt['is_weekday'].groupby(tt['userId']).mean()
    
    def weekday_decide(x):
        if x >= 5.0/7: weekday = True
        else: weekday = False
        return weekday
    
    week_av = week_av.apply(weekday_decide)
    arradata['av_post_weekday'] = week_av
    most_common_weekday = week_av.value_counts().idxmax()
    arradata['av_post_weekday'] = arradata['av_post_weekday'].fillna(most_common_weekday)
    
    tod = tt['time_of_day'].groupby([tt['userId']]).mean()
    most_common_tod = tod.value_counts().idxmax()
        
    arradata['av_time_of_day'] = tod
    arradata['av_time_of_day'] = arradata['av_time_of_day'].fillna(most_common_tod)
        
    #
    #Add SNA metrics
    #
    
    arradata = add_sna_metrics(arradata)
    
    #
    #encode categorical columns
    #
    
    arradata = convert_columns(arradata)
    
    arradata.to_pickle('arradatafile')
    
    return arradata