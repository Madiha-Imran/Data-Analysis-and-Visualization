import facebook
import pandas as pd
from google.cloud import storage
from google.cloud import pubsub_v1
import os
from io import StringIO
import gcsfs

apikey = "apikey here!"

def all_complete(event, context):

    #Reading fb data
    fb = facebook.GraphAPI(apikey)
    #profile = fb.get_object ("me", fields = "name, birthday")
    likes = fb.get_object("me", fields = "likes.limit(100) {name,about,fan_count,category}")
    
    pages = likes['likes']['data']
    pages_df = pd.DataFrame(pages)
        
    #reading category.csv file from storage
    storage_client = storage.Client()
    bucket_name = "category_csv_file"

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob("fb_page_category.csv")

    category = pd.read_csv("gs://category_csv_file/fb_page_category.csv")
    category = pd.DataFrame(category)
    
    #merging category and likedpages
    merged_df = pd.merge(pages_df, category, how='left', left_on='category', right_on='SubCategory')
    mainCategoryNo = merged_df['MainCategory'].value_counts()
    
    #Writing the DF as csv file in gcs bucket
    cat_csv = StringIO()
    mainCategoryNo.to_csv(cat_csv)
    cat_csv.seek(0)
    storage_client.get_bucket('category_csv_file').blob('fb_category_count.csv').upload_from_file(cat_csv, content_type='text/csv')
    
    #Writing the DF as JSON file in gcs bucket
    cat_json = StringIO()
    mainCategoryNo.to_json(cat_json)
    cat_json.seek(0)
    storage_client.get_bucket('category_csv_file').blob('category_count.json').upload_from_file(cat_json, content_type='text/json')

    #Publishing message in pubsub topic to newtopic

    topic_name = "projects/iot-project-group6/topics/newtopic"
   
    publisher = pubsub_v1.PublisherClient()
    publisher.publish(topic_name, b"gs://category_csv_file/category_count.json", spam='eggs')

    #top 10 pages based on fan count as CSV
    top_ten = pages_df.nlargest(10, 'fan_count')[["name","fan_count"]]
    top_ten_csv = StringIO()
    top_ten.to_csv(top_ten_csv)
    top_ten_csv.seek(0)
    storage_client.get_bucket('category_csv_file').blob('top_ten_pages.csv').upload_from_file(top_ten_csv, content_type='text/csv')

    #top 10 pages based on fan count As JSON
    top_ten = pages_df.nlargest(10, 'fan_count')[["name","fan_count"]]
    top_ten_json = StringIO()
    top_ten.to_json(top_ten_json)
    top_ten_json.seek(0)
    storage_client.get_bucket('category_csv_file').blob('top_ten_pages.json').upload_from_file(top_ten_json, content_type='text/json')

    #Publishing message in pubsub topic to top10pages topic

    topic_name = "projects/iot-project-group6/topics/top10pages"
   
    publisher = pubsub_v1.PublisherClient()
    publisher.publish(topic_name, b"gs://category_csv_file/top_ten_pages.json", spam='eggs')


    #analysis of liked pages based on fan count
    df_fan_count = merged_df[["name","fan_count","MainCategory"]]
    df_fan_count["fan_count"] = pd.cut(df_fan_count["fan_count"], [0,500,5000,10000,100000000],labels= ['low(<500)','average(501-5000)','high(5001-10000)','very high(>10000)'])
    fan_count_group = df_fan_count.groupby("MainCategory").fan_count.value_counts()

     #Writing fan counf DF into csv file in gcs bucket
    fan_df = pd.DataFrame(fan_count_group)
    fan_df.columns=['page_counts']
    fan_count = StringIO()
    fan_df.to_csv(fan_count)
    fan_count.seek(0)
    storage_client.get_bucket('category_csv_file').blob('fan_count.csv').upload_from_file(fan_count, content_type='text/csv')
    