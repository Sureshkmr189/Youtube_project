from googleapiclient.discovery import build # type: ignore
import pymongo # type: ignore
import psycopg2 # type: ignore
import pandas as pd # type: ignore
import streamlit as st # type: ignore

# Api connection setup:

def api_connect():
    api_key="AIzaSyA783tw90T5_kOBoOvxL8r64xrCs4PA6O8"
    api_service_name="Youtube"
    api_version="V3"
    
    youtube=build(api_service_name,api_version,developerKey=api_key) #connect using build()

    return youtube

youtube=api_connect()



#channel information:
def get_channel_info(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
    )
    response = request.execute()#executes the request and stores the response from the YouTube Data API

    for items in response["items"]: #indexing is not necessary since we using for loop.
        data_collected = {
            "Channel_name": items["snippet"]["title"],
            "Channel_id": items["id"],
            "Channel_Description":items["snippet"]["description"],
            "Subscribers":items["statistics"]["subscriberCount"],
            "Total_views":items["statistics"]["viewCount"],
            "Total_videos":items["statistics"]["videoCount"],
            "Playlist_id":items["contentDetails"]["relatedPlaylists"]["uploads"]} 
# extracts various pieces of information about the channel from the snippet and statistics sections of the response, such as the channel name, description, ID, subscriber count, view count, video count, and uploads playlist ID. It stores this information in a dictionary named
    return data_collected


# video ids:
def get_videos_ids(channel_id):
    video_ids=[]

    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'] # index used becoz there is a list in 'items'

    next_page_token=None

    while True: # to access all next_page_token in all videos(continue access to get total videosid )
        response1 = youtube.playlistItems().list(
                                                part="snippet",
                                                playlistId=Playlist_id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute() # maxResults to get the maximum video id , the default is 5

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'] )
        
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None: # using if condition , once all videos are completed and becomes none .it breaks
            break
    return video_ids


# video information

def get_video_info(video_ids):

    video_data=[] # empty lis to get a complete data in a loop

    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id        
        )

        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item["snippet"]["channelId"],
                    Video_Id=item["id"],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'), # use .get('tags') function if we get an "key error "
                    Thumbnail=item['snippet']["thumbnails"]["default"]["url"],
                    Description=item['snippet'].get("description"),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Total_Views=item['statistics'].get('viewCount'),
                    Likes=item["statistics"].get('likeCount'),
                    Total_Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data


# comment information:
def get_comment_info(video_idss): # *** 4th/final step ***

    Comment_data=[] # ***3rd step***

    try: # try and except used because comment might be disabled for some videos
        for video_id in video_idss: 
            request = youtube.commentThreads().list(    # ***1st step***
                part="snippet",
                maxResults=50,
                videoId=video_id
                )
            response = request.execute()

            for item in response['items']: # ***2nd step****
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published_at=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)

    except:
        pass

    return Comment_data


# playlist information :

def get_playlist_details(channel_id):
    next_page_token=None
    All_Data=[]

    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50, # to get 50 playlists 
                pageToken=next_page_token  # use if playlists are more than 50

            )
        response = request.execute()

        for item in response['items']:
            data=dict(Playlist_ID=item['id'],
                    Title=item['snippet']['title'],
                    Channel_ID=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount']            
                    )
            All_Data.append(data)
            
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break

    return All_Data


 #connection string 
client=pymongo.MongoClient("mongodb+srv://sureshkmr189:Qazxc123@cluster0.flizxlr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data"]  


def channel_details(channel_id):  # passing channel id here 
    ch_details=get_channel_info(channel_id) # we get channel details here
    pl_details=get_playlist_details(channel_id) # we get playlist details here
    vi_ids=get_videos_ids(channel_id)  # we get video ids here
    vi_Details=get_video_info(vi_ids)  # get video Ids from the above variable vi_ids
    com_details=get_comment_info(vi_ids) # comment details 

    coll1=db["channel_details"]
    coll1.insert_one({"Channel_information":ch_details,
                      "Playlist_information":pl_details,
                      "Video_information":vi_Details,
                      "Comment_information":com_details})
    return "upload completed sucessfully"  

def channels_table(channel_name_s): #function is created in order to avoid running all times
    # 1. Table creation for channels , playlist, comments and videos (8th video)
    mydb = psycopg2.connect(host='localhost',  
                            user='postgres',
                            password='Qazxc@123',
                            database='youtube_data',
                            port=5432)
    cursor=mydb.cursor()

    
    create_query='''create table if not exists channels(Channel_name varchar(100),
                                                    Channel_id varchar(80) primary key,
                                                    Channel_Description text,
                                                    Subscribers bigint,
                                                    Total_views bigint,
                                                    Total_videos int,
                                                    Playlist_id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()


    single_channel_details=[]  #step 1  getting data from mongoDB
    db=client["Youtube_data"] # calling db and collection form mongoDB
    coll1=db["channel_details"]
    for ch_data in coll1.find({"Channel_information.Channel_name": channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data['Channel_information'])
    df_single_channel_details=pd.DataFrame(single_channel_details)  # converting in to dataframe channel list


    # 3.insert rows in table: (10th video)
    for index,row in df_single_channel_details.iterrows():  
        insert_query='''insert into channels(Channel_name,
                                            Channel_id,
                                            Channel_Description,
                                            Subscribers,
                                            Total_views,
                                            Total_videos,
                                            Playlist_id)
                                            

                                            values(%s,%s,%s,%s,%s,%s,%s)'''## columns names in table (pstgre) and allocating values using %s
        values=(row['Channel_name'],
                row['Channel_id'],
                row['Channel_Description'],
                row['Subscribers'],
                row['Total_views'],
                row['Total_videos'],
                row['Playlist_id'])# column name in dataframe
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            news=f"Provided channel name {channel_name_s} is already exists"
            return news

            



def playlist_table(channel_name_s):
# strep 1 : connection and create table playlists:
    mydb = psycopg2.connect(host='localhost',  
                            user='postgres',
                            password='Qazxc@123',
                            database='youtube_data',
                            port=5432)

    cursor=mydb.cursor()

    create_query='''create table if not exists playlists(Playlist_ID varchar(100) primary key,
                                                    Title varchar(500),
                                                    Channel_ID varchar(100),
                                                    Channel_Name varchar(100),
                                                    PublishedAt timestamp,
                                                    Video_Count int
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()

    # 2.Get data from mongo (9th video)
    single_playlist_details=[]
    db=client["Youtube_data"] # calling db and collection form mongoDB
    coll1=db["channel_details"]
    for ch_data in coll1.find({"Channel_information.Channel_name": channel_name_s},{"_id":0}):
        single_playlist_details.append(ch_data["Playlist_information"])
    df_single_playlist_details=pd.DataFrame(single_playlist_details[0])

    

# 3.insert rows in table: (10th video)
    for index,row in df_single_playlist_details.iterrows():  
        insert_query='''insert into playlists(Playlist_ID,
                                            Title,
                                            Channel_ID,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count )
                                            

                                            values(%s,%s,%s,%s,%s,%s)'''## columns names in table (pstgre) and allocating values using %s
        values=(row['Playlist_ID'],
                row['Title'],
                row['Channel_ID'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count'],
                )                          
        

        cursor.execute(insert_query,values)
        mydb.commit()
  


def videos_table(channel_name_s):
        # 1.create videos table
        mydb = psycopg2.connect(host='localhost',  
                                user='postgres',
                                password='Qazxc@123',
                                database='youtube_data',
                                port=5432)

        cursor=mydb.cursor()


        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(100),
                                                        Tags text, 
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Total_Views bigint,
                                                        Likes bigint,
                                                        Total_Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar (50)
                                                        )'''
        cursor.execute(create_query)
        mydb.commit()


        #step 2  getting data from mongoDB
        single_video_details=[]
        db=client["Youtube_data"] # calling db and collection form mongoDB
        coll1=db["channel_details"]
        for ch_data in coll1.find({"Channel_information.Channel_name": channel_name_s},{"_id":0}):
            single_video_details.append(ch_data["Video_information"])

        df_single_video_details=pd.DataFrame(single_video_details[0]) 


        for index,row in df_single_video_details.iterrows():  
                insert_query='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags, 
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Total_Views,
                                                Likes,
                                                Total_Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status
                                                ) 
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''## columns names in table (pstgre) and allocating values using %s
                values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Total_Views'],
                        row['Likes'],
                        row['Total_Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'] 
                        )# column name in dataframe (df1)
                cursor.execute(insert_query,values)
                mydb.commit()


def comments_table(channel_name_s):
        # 1.create playlist table
        mydb = psycopg2.connect(host='localhost',  
                                user='postgres',
                                password='Qazxc@123',
                                database='youtube_data',
                                port=5432)

        cursor=mydb.cursor()

        
        create_query='''create table if not exists comments(Comment_Id varchar(50) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_text text,
                                                        Comment_Author varchar(100),
                                                        Comment_published_at timestamp
                                                        )'''
        cursor.execute(create_query)
        mydb.commit()


        #step 2  getting data from mongoDB
        single_comments_details=[]
        db=client["Youtube_data"] # calling db and collection form mongoDB
        coll1=db["channel_details"]
        for ch_data in coll1.find({"Channel_information.Channel_name": channel_name_s},{"_id":0}):
                single_comments_details.append(ch_data["Comment_information"])
        df_single_comments_details=pd.DataFrame(single_comments_details[0]) 

        #step3 insert into table(postgre)

        for index,row in df_single_comments_details.iterrows():  
                insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_text,
                                                Comment_Author,
                                                Comment_published_at
                                                )
                                                values(%s,%s,%s,%s,%s)'''## columns names in table (pstgre) and allocating values using %s
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_text'],
                        row['Comment_Author'],
                        row['Comment_published_at']
                        )# column name in dataframe (df3)
                        
                
                cursor.execute(insert_query,values)
                mydb.commit()


def tables(single_channel):
    news= channels_table(single_channel)
    if news:
        return news
    else:
        playlist_table(single_channel)    
        videos_table(single_channel)
        comments_table(single_channel)

        return ("Tables created sucessfully")


def show_channel_table():
    ch_list=[] 
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"Channel_information":1}): 
        ch_list.append(ch_data['Channel_information'])
    df=st.dataframe(ch_list)

    return df


def show_playlists_table():
    pl_list=[]  #step 1  getting data from mongoDB
    db=client["Youtube_data"] # calling db and collection form mongoDB
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"Playlist_information":1}):
        for i in range(len(pl_data['Playlist_information'])):
            pl_list.append(pl_data['Playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list=[] 
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"Video_information":1}):
        for i in range(len(vi_data['Video_information'])):
            vi_list.append(vi_data['Video_information'][i])
    df2=st.dataframe(vi_list)

    return df2


def show_comments_table():
    comm_list=[] 
    db=client["Youtube_data"] # calling db and collection form mongoDB
    coll1=db["channel_details"]
    for comm_data in coll1.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(comm_data['Comment_information'])):
            comm_list.append(comm_data['Comment_information'][i])
    df3=st.dataframe(comm_list) # converting in to dataframe "playlist"

    return df3


# Creating sidebar in streamlit

with st.sidebar:
    st.image("https://img.naidunia.com/naidunia/ndnimg/28022023/28_02_2023-youtube_down.jpg")
    st.title(":blue[YouTube Data Harvesting and Warehousing using SQL and Streamlit]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Managment using MongoDB and SQL")

Channel_id=st.text_input("Enter Channel ID")


if st.button("Collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"Channel_information":1}):
        ch_ids.append(ch_data["Channel_information"]["Channel_id"]) # use slicing

    if Channel_id in ch_ids:
        st.success("Channel details of the given ID is already exists")
    else:
        insert=channel_details(Channel_id)
        st.success((insert))

# creating a list to select a particular:
all_channels=[]  
db=client["Youtube_data"]
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"Channel_information":1}):
    all_channels.append(ch_data["Channel_information"]["Channel_name"])

unique_channel=st.selectbox("Select the channel",all_channels) # select the particular channel to migrate once collected and stored data in mongoDB


if st.button("Migrate to SQL"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_playlists_table()

elif show_table=="COMMENTS":
    show_comments_table()


#Sql Connection query
mydb = psycopg2.connect(host='localhost',  
                        user='postgres',
                        password='Qazxc@123',
                        database='youtube_data',
                        port=5432)
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1.Names of all the videos and their corresponding channels",
                                             "2.Channels with most number of videos",
                                             "3.Top 10 most viewed videos and their respective channels",
                                             "4.Comments in each video",
                                             "5.Videos with highest likes",
                                             "6.Total number of likes",
                                             "7.Total number of views for each channel",
                                             "8.Names of all the channels that have published videos in the year 2022",
                                             "9.Duration of all videos in each channel",
                                             "10.Videos with the highest number of comments"))


if question=="1.Names of all the videos and their corresponding channels":  
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["Video Title","Channel Name"])
    st.write(df1)


elif question=="2.Channels with most number of videos":  
    query2='''select channel_name as ChannelName,total_videos as no_of_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel Name","No of Videos"])
    st.write(df2)


elif question=="3.Top 10 most viewed videos and their respective channels":  
    query3='''select total_views as Views,channel_name as ChannelName,title as Videotitle from videos
            where total_views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Views","Channel Name","Video title"])
    st.write(df3)


elif question=="4.Comments in each video":  
    query4='''select Total_Comments as no_of_comments, title as videotitle from videos 
            where Total_Comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["Total Comments","Video Title"])
    st.write(df4)


elif question=="5.Videos with highest likes":  
    query5='''select title as title , channel_name as channelname, likes as Totallikes from videos 
            where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Video Title","Channel name","likes count"])
    st.write(df5)


elif question=="6.Total number of likes":  
    query6='''select likes as likecount, title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likes Count","video title"])
    st.write(df6)


elif question=="7.Total number of views for each channel":  
    query7='''select channel_name as channelname,total_views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel Name","Total views"])
    st.write(df7)


elif question=="8.Names of all the channels that have published videos in the year 2022":
    query8='''select title as video_title,published_date as video_release,channel_name as channel_name from videos
            where extract (year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Video Title","Published Date", "Channel Name"])
    st.write(df8)


elif question=="9.Duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as average_duration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channel Name","Average Duration"])
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["Channel Name"]
        average_duration=row["Average Duration"]
        average_duration_str=str(average_duration)
        T9.append(dict(Channel_Name=channel_title,avg_duration=average_duration_str))
    df99=pd.DataFrame(T9)
    st.write(df99)


elif question=="10.Videos with the highest number of comments":
    query10='''select title as videotitle,channel_name as channelname,total_comments as comments from videos
            where total_comments is not null order by total_comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Video Title","Channel Name", "No of Comments"])
    st.write(df10)
