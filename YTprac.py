import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import numpy as np
from pymongo import MongoClient
import pymysql
import time
from datetime import datetime
import matplotlib.pyplot as plt

# Streamlit Tab Title
st.set_page_config(page_title='YT Scrapper')

# Youtube API Activating

#api_key ='AIzaSyBGDgcEWkoJn6ZLyTu0g7_P7ziagQDIjZQ' - QE 27/05
#api_key ='AIzaSyCaaoxSkYn_Pq_7KC5DGFlvXk0elqQiUKg' 
api_key='AIzaSyDWMR_rH6-nf_ebBKaK6iD-xGFYyGG1fhM'
#api_key ='AIzaSyDv3L3_VeV3NwOQqAQVq1C_j0QC3PG5CcA'
#api_key ='AIzaSyBGvhoZsiYdBytd2LuG8bzqpvMBdCLNRx4'

#youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=credentials)
youtube = build('youtube','v3',developerKey=api_key) 



st.header('YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit')
st.write('''This app is a Youtube Scraping web app created using Streamlit. 
        It scrapes the Youtube data for the given ChannelID. 
        The details can be exported to MongoDB.''')

with st.form(key='form1'):
    # Getting No. of channels
    n = st.number_input('Enter the number of youtube data to be Searched: (MAX=10)')

    # Conditions of No.of channels
    while (n>10):
        st.error('Error! You can search only upto 10 Youtube channel')
        n = st.number_input('Enter the number of youtube data to be Searched: (MAX=10)')
    if(n.is_integer()==False):
        st.error('Error! Enter an Integer from 1-10')
        n = st.number_input('Enter the number of youtube data to be Searched: (MAX=10)')

    n=round(n)
    st.write('the Number of channels to be searched is',n)

    # Getting Channel ID 
    channel_id = []
    for i in range(n):
        if i == 0:
            st.write(i+1,'st Channel ID')
            channel_id.append(st.text_input(label='Enter Below',key=i))
        elif i == 1:
            st.write(i+1,'nd Channel ID')
            channel_id.append(st.text_input(label='Enter Below',key=i))
        elif i == 2:
            st.write(i+1,'rd Channel ID')
            channel_id.append(st.text_input(label='Enter Below',key=i))
        else:
            st.write(i+1,'th Channel ID')
            channel_id.append(st.text_input(label='Enter Below',key=i))
    # Creating Submit Button 
    submit_button = st.form_submit_button(label="Submit Channel IDs")

if submit_button:
    channel_id = pd.DataFrame(channel_id)
    #st.success('Succesfully Added Channel IDs')
    st.success(f"Succesfully Added {len(channel_id)} Channel IDs ")
    st.experimental_rerun()
    

# Displaying ChannelID
st.markdown('_Channel ID Entered shown Below_')
st.dataframe(channel_id,width=150, height=150)
Cid = list(channel_id)

# Fn to get Channel Details
def get_channel_stats(youtube, c_ids):
    all_data = []

    for c_id in c_ids:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=c_id
        )
        response = request.execute()

        channel_data = response['items'][0]

        data = {
            'channel_name': channel_data['snippet']['title'],
            'channel_Description': channel_data['snippet']['description'],
            'subs_count': channel_data['statistics'].get('subscriberCount', 0), # to avoid getting NULL error
            'views': channel_data['statistics'].get('viewCount', 0), # to avoid getting NULL error
            'tot_vid': channel_data['statistics'].get('videoCount', 0), # to avoid getting NULL error
            'playlist_id': channel_data['contentDetails']['relatedPlaylists']['uploads']
        }
        all_data.append(data)

    return all_data

# Calling the fn to get the channel Details
c_stat = get_channel_stats(youtube,channel_id)

# Converting it to DataFrame
c_data = pd.DataFrame(c_stat)

# Converting objects to Integers
c_data['subs_count']=pd.to_numeric(c_data['subs_count'])
c_data['views']=pd.to_numeric(c_data['views'])
c_data['tot_vid']=pd.to_numeric(c_data['tot_vid'])

# Saving the Channel Name for futher use
titlee = c_data['channel_name']
titleee=list(titlee)

# Saving the playlist of all channels
playlist_id = c_data['playlist_id'].iloc[0:len(c_data)]
playlist_id=list(playlist_id)

# Fn to get Video ID
def get_video_ids(youtube,playlist_id):
    
    request = youtube.playlistItems().list(
        part = 'contentDetails',
        playlistId = playlist_id,
        maxResults =50 # Shows 1st 50 playlist id's 
    )
    response = request.execute() # response has all the details related to videoId
    
    # Now, we are going to get all the video_ID from the playlist_Id (not only 50)
    video_ids =[]
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    
    next_page_token = response.get('nextPageToken') # used get method instead of response['nextPageToken'] since .get doesnt give error but [] gives error if false
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part = 'contentDetails',
                playlistId = playlist_id,
                maxResults =50, # Shows 1st 50 playlist id's 
                pageToken = next_page_token
            )
            response = request.execute()
            
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
            next_page_token = response.get('nextPageToken')
        
    return video_ids

# Calling the fn to get Video IDs 
v_id=[]
for i in range(len(channel_id)):
    v_id.append(get_video_ids(youtube,playlist_id[i]))

# Merging Channel ID with corresponding VideoIDs
vid_id = pd.DataFrame(list(zip(playlist_id,v_id)))
# Giving Heading to the columns
vid_id.columns=['Playlist','VideoID']

# Creating 2 columns in web interface
col1,col2 = st.columns(2)

with col1:
    # Displaying the Channel Details
    st.subheader('Channel Table')
    st.dataframe(c_data)
with col2:
    # Displaying the VideoIds
    st.subheader('Video ID Table')
    st.dataframe(vid_id)

# Fn to get Video Details
def get_video_details(youtube, video_ids):
    all_video_stats = []
    
    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
            part = 'snippet,statistics',
            id =','.join(video_ids[i:i+50]) # According to youtube guidelines, we can pass only 50 videos at a time 
        )
        response = request.execute()
    
        for video in response['items']:
            video_stats = dict(Title = video['snippet']['title'],
                            Desc = video['snippet']['description'],
                            Published_date = video['snippet']['publishedAt'],
                            Views = video['statistics']['viewCount'],
                            likes = video['statistics'].get('likeCount',0),
                            #DisLikes = video['statistics']['dislikeCount'], - Dislike has been blocked by YouTube Guidelines
                            Comments = video['statistics'].get('commentCount',0),
                            VideoID = video['id']
                            )
            all_video_stats.append(video_stats)
    return all_video_stats

# Calling the fn to get Video Details 
video_details=[]
for i in range(len(v_id)):
    video_details.append(get_video_details(youtube, v_id[i]))

# Converting it to DataFrame
video_data = pd.DataFrame(video_details)

# Getting the transpose for easy access
video_data = video_data.T

# Function to extract dictionary values - becoz the video data stores the detail in the form of dictionary
def extract_dict_value(dictionary, key):
    if isinstance(dictionary, dict) and key in dictionary:
        return dictionary[key]
    else:
        return None

# Create a new dataframe with references to dictionary values
tit1 = video_data.applymap(lambda cell: extract_dict_value(cell, 'Title'))
desc = video_data.applymap(lambda cell: extract_dict_value(cell, 'Desc'))
videoID = video_data.applymap(lambda cell: extract_dict_value(cell, 'VideoID'))
pd1 = video_data.applymap(lambda cell: extract_dict_value(cell, 'Published_date'))
v1 = video_data.applymap(lambda cell: extract_dict_value(cell, 'Views'))
likes = video_data.applymap(lambda cell: extract_dict_value(cell, 'likes'))
c1 = video_data.applymap(lambda cell: extract_dict_value(cell, 'Comments'))
tit1.replace(np.nan,0) 
desc.replace(np.nan,0) 
videoID.replace(np.nan,0)
pd1.replace(np.nan,0) 
v1.replace(np.nan,0) 
c1.replace(np.nan,0)
# Creating a single Library containing all details  
tit1 = {'title':tit1,
        'desc' :desc,
        'VideoID':videoID,
        'published Date':pd1,
        'Views':v1,
        'Likes':likes,
        'Comments':c1
    }

v_data=[]

# Converting the library to dataframe
for i in range(len(v_id)):
    v_data.append(pd.DataFrame({
        'title': tit1['title'][i],
        'Description' : tit1['desc'][i],
        'VideoID':tit1['VideoID'][i],
        'published Date': tit1['published Date'][i],
        'Views': tit1['Views'][i],
        'Likes': tit1['Likes'][i],
        'Comments': tit1['Comments'][i]
    }))

# Removing all NA Values in each Channel's Data frame
for i in range(len(v_id)):
    v_data[i].dropna(inplace = True)

# Converting objects to Integers
for i in range(len(v_data)):
    v_data[i]['Views']=pd.to_numeric(v_data[i]['Views'])
    v_data[i]['Likes']=pd.to_numeric(v_data[i]['Likes'])
    v_data[i]['Comments']=pd.to_numeric(v_data[i]['Comments'])

# Displaying the Tables
st.subheader('Video Details Table')
for i in range(len(v_id)):
    st.write(c_data['channel_name'][i])
    st.dataframe(v_data[i])

# Fn to get Comments
def get_com(youtube, video_ids):
    cmt = []

    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_ids,
        maxResults=50
    )
    response = request.execute()

    for i in range(len(response['items'])):
        ct = {
            'ComID' : response['items'][i]['snippet']['topLevelComment'].get('etag',0),
            'Video_ID': response['items'][i]['snippet'].get('videoId',0),
            'Author': response['items'][i]['snippet']['topLevelComment']['snippet'].get('authorDisplayName', None),
            'Comments': response['items'][i]['snippet']['topLevelComment']['snippet'].get('textDisplay', None),
            'CDate' : response['items'][i]['snippet']['topLevelComment']['snippet'].get('publishedAt', None)
        }
        cmt.append(ct)

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_ids,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for i in range(len(response['items'])):
                ct = {
                    'ComID' : response['items'][i]['snippet']['topLevelComment'].get('etag',0),
                    'Video_ID': response['items'][i]['snippet'].get('videoId', 0),
                    'Author': response['items'][i]['snippet']['topLevelComment']['snippet'].get('authorDisplayName', None),
                    'Comments': response['items'][i]['snippet']['topLevelComment']['snippet'].get('textDisplay', None),
                    'CDate' : response['items'][i]['snippet']['topLevelComment']['snippet'].get('publishedAt', None)
                }
                cmt.append(ct)

            next_page_token = response.get('nextPageToken')

    return cmt

st.subheader('Comments Table')

# Calling the fn and storing it in comments
comments = []
for i in range(len(v_id)):
    cntr=[]
    for j in range(len(v_id[i])):
        #current_comments = get_com(youtube, v_id[0][i])
        cntr.append(get_com(youtube,v_id[i][j]))
    comments.append(cntr)

# each cell in comments is in dictionary format, we are structuring it channelwise with each channel as videoID and comments
comi=[]
comv=[]
comc=[]
coma=[]
comd=[]
for i in range(len(v_id)):
    tempi=[]
    tempv=[]
    tempc=[]
    tempa=[]
    tempd=[]
    for j in range(len(comments[i])):
        for k in range(len(comments[i][j])):
            tempi.append(comments[i][j][k]['ComID'])
            tempv.append(comments[i][j][k]['Video_ID'])
            tempc.append(comments[i][j][k]['Comments'])
            tempa.append(comments[i][j][k]['Author'])
            tempd.append(comments[i][j][k]['CDate'])
    comi.append(tempi)
    comv.append(tempv)
    comc.append(tempc)
    coma.append(tempa)
    comd.append(tempd)

# join 2 list (VideoID, Comments) as a single DataFrame
com=[]
for i in range(len(v_id)):
    com.append(pd.DataFrame(list(zip(comi[i],comv[i],coma[i],comc[i],comd[i]))))

# Assigning Column Names
for i in range(len(v_id)):
    com[i].columns=['CommentID','VideoID','Author','Comments','PublishedDate']

# Displaying the comments
for i in range(len(v_id)):
    st.write(titlee[i])
    st.dataframe(com[i])



# Now we are moving to MongoDB -Connecting to MongoDB local Host
client = MongoClient('mongodb://localhost:27017/')
db = client['YT'] 
collection = db['Channed_ID']
collection.delete_many({})
client.drop_database('YT')
client.close()

client = MongoClient('mongodb://localhost:27017/')
db = client['YT'] 
collection = db['Channed_ID']

#if 'dub' not in st.session_state:
#    st.session_state.flag=0
# Variable created to ensure linear traverse
flag=0

# Creating Choice for exporting the Data
option = ["yes","no"]
choice = st.selectbox('Are you done with extracting the Youtube Data', option, index=1)

if choice=="yes":

    collection.delete_many({})

    st.header("Exporting Data to MongoDB")
 
    # Creating a dictionary to store Channelwise Channel Data
    channel_data ={}
    for i in range(len(c_data)):
        channel_data[i] = {
                'Channel_ID' : Cid[i],
                'channel_name': c_data['channel_name'][i],
                'channel_desc': c_data['channel_Description'][i],
                'subs_count': c_data['subs_count'][i],
                'Views' : c_data['views'][i],
                'tot_vid': c_data['tot_vid'][i],
                'playlist_id': c_data['playlist_id'][i]
                }
    
    # Creating a dictionary to store Channelwise Video Details
    temp={}
    vd=[]
    for i in range(len(v_data)):
        temp = {}
        for j in range(len(v_data[i])):
            temp[j] = {
            'Video Title': v_data[i]['title'][j],
            'VideoDesc':v_data[i]['Description'][j],
            'Video ID': v_data[i]['VideoID'][j],
            'Published':v_data[i]['published Date'][j],
            'Views': v_data[i]['Views'][j],
            'Likes': v_data[i]['Likes'][j],
            'Comments': v_data[i]['Comments'][j]
            }
        vd.append(temp)

    # Inserting Comments dictionary inside the corresponding Video dictionary - nested dictionary
    comm=[]
    for i in range(len(v_data)):
        tent1=[]
        for j in range(len(v_data[i])):
            tent={}
            for k in range(len(com[i])):
                if v_data[i]['VideoID'][j]== com[i]['VideoID'][k]:
                    tent[k]={
                        'CommentID':com[i]['CommentID'][k],
                        'Author' :com[i]['Author'][k],
                        'Comment_Text':com[i]['Comments'][k],
                        'PublishedAt': com[i]['PublishedDate'][k]
                        }
            tent1.append(tent)
        comm.append(tent1)

    # Inserting the nested dictionary inside the corresponding Channel dictionary
    vidcom=[]
    for i in range(len(v_data)):
        arb={}
        for j in range(len(comm[i])):
            arb[j] = {
                f'Video_Id{j}':vd[i][j],
                'Comments':comm[i][j]
            }
        vidcom.append(arb)

    # Creating a dictionary to combine Both the Channel & Nested dictionary Details Channelwise
    exdata={}
    for i in range(len(v_id)):
        exdata[i] = {
                'ChannelData':channel_data[i],
                'VideoData':vidcom[i]
            }

    st.write(exdata)

    # Creating a Choice to choose whether to store data in MongoDB
    yn=["y","n"]
    imp = st.selectbox('Do you want to store it in the data lake ', yn, index=1)

    if imp=="y":

        # fn to convert_numpy_int64
        def convert_numpy_int64(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, dict):
                return {str(key): convert_numpy_int64(value) for key, value in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert_numpy_int64(item) for item in obj]
            return obj

        # Calling the fn to avoid int64 Error
        export = convert_numpy_int64(exdata)

        # Insering the data to MongoDB
        collection.insert_one(export)
        st.success('Channel data inserted successfully!')
        flag+=1


if flag==1:  
    
    # Establishing a connection btw MongoDB & Python
    mongo_client = MongoClient('mongodb://localhost:27017')
    mongo_db = mongo_client['YT']
    mongo_collection = mongo_db['Channed_ID']

    # Storing the MongoDB to a cursor called document
    document = mongo_collection.find()
    sup=document[0]

    # Establishing a connection btw MYSQL & Python   
    myconnection = pymysql.connect(host='localhost', user='root', password='Muthu@123', autocommit=True )
    cursor = myconnection.cursor()


    # Create the MySQL database if it doesn't exist
    cursor.execute("CREATE DATABASE IF NOT EXISTS ytscrape")
    cursor.execute("use ytscrape")
    myconnection.commit()

    # Create SQL table Channel
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel(
            channelIDb varchar(255),
            channelName1 varchar(255),
            channelDescription text,
            subsCount int ,
            views int,
            totVid int ,
            playlistID varchar(255), 
            INDEX idx_playlistID (playlistID)
        );
    """)
    
    # Create SQL table Video
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Video(
            playlistIDr varchar(255),
            VideoTitle varchar(255),
            VideoDesc text,
            VideoID varchar(255) PRIMARY KEY,
            PublishedDate DATE,
            Views int ,
            Likes int,
            Comments int,
            FOREIGN KEY (playlistIDr) REFERENCES channel(playlistID)
        )
    """)

    # Create SQL table Comments
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Com(
            CommentID varchar(255),
            VideoID varchar(255),
            comments text,
            author varchar(255),
            Published_date Date,
            FOREIGN KEY (VideoID) REFERENCES Video(VideoID)
        )
    """)

    myconnection.commit() 

    # Defining a fn Mitigate to export data to MYSQL
    def mitigate(CName):
        if len(st.session_state.dub)>0:
            st.session_state.dub.pop(inox)
            i=0
            st.session_state.h='0'
            for i in range(len(sup)-1):
                i=str(i)
                if (sup[i]['ChannelData']['channel_name'] == CName):
                    st.session_state.h=i
                    break

            st.session_state.h=str(st.session_state.h)

            # Storing all the Comments data from MONGODB as a dataframe rass
            ml=0
            rass=[]
            rci=[]
            rvi=[]
            rau=[]
            rct=[]
            rpa=[]
            for i in range(len(sup[st.session_state.h]['VideoData'])):
                for j in range(len(sup[st.session_state.h]['VideoData'][str(i)]['Comments'])):
                    rci.append(sup[st.session_state.h]['VideoData'][str(i)]['Comments'][str(ml)]['CommentID'])
                    rvi.append(sup[st.session_state.h]['VideoData'][str(i)][f'Video_Id{i}']['Video ID'])
                    rau.append(sup[st.session_state.h]['VideoData'][str(i)]['Comments'][str(ml)]['Author'])
                    rct.append(sup[st.session_state.h]['VideoData'][str(i)]['Comments'][str(ml)]['Comment_Text'])
                    rpa.append(sup[st.session_state.h]['VideoData'][str(i)]['Comments'][str(ml)]['PublishedAt']) 
                    ml+=1
            rass.append(pd.DataFrame(list(zip(rci,rvi,rau,rct,rpa))))
            rass[0].columns=['CommentID','VID','Author','Comments','Published']

            # Storing all the Channel data from MONGODB to Variables
            channel_ida  = sup[st.session_state.h]['ChannelData']['Channel_ID']
            channel_name = sup[st.session_state.h]['ChannelData']['channel_name']
            channelDesc = sup[st.session_state.h]['ChannelData']['channel_desc']
            subs_count = sup[st.session_state.h]['ChannelData']['subs_count']
            views = sup[st.session_state.h]['ChannelData']['Views']
            tot_vid = sup[st.session_state.h]['ChannelData']['tot_vid']
            playlist_id = sup[st.session_state.h]['ChannelData']['playlist_id']

            # Inserting all channel details inside channel table
            cursor.execute("""
            insert into channel
            (channelIDb,
            channelName1,
            channelDescription,
            subsCount,
            views,
            totVid,
            playlistID
            )
            VALUES (%s, %s, %s, %s, %s, %s,%s)
            """, (channel_ida,channel_name,channelDesc,subs_count,views,tot_vid,playlist_id))

            # Storing all the Video Data from MongoDB to variables
            playlist_id = sup[st.session_state.h]['ChannelData']['playlist_id']
            Mvt=[]
            Mvd=[]
            Mvid=[]
            Mpd=[]
            Mviews=[]
            Mlikes=[]
            Mcomments=[]
            for i in range(len(sup[st.session_state.h]['VideoData'])):
                Mvt.append(sup[st.session_state.h]['VideoData'][str(i)][f'Video_Id{i}']['Video Title'])
                Mvd.append(sup[st.session_state.h]['VideoData'][str(i)][f'Video_Id{i}']['VideoDesc'])
                Mvid.append(sup[st.session_state.h]['VideoData'][str(i)][f'Video_Id{i}']['Video ID'])
                Mpd.append(datetime.strptime(sup[st.session_state.h]['VideoData'][str(i)][f'Video_Id{i}']['Published'], "%Y-%m-%dT%H:%M:%SZ"))
                Mviews.append(sup[st.session_state.h]['VideoData'][str(i)][f'Video_Id{i}']['Views'])
                Mlikes.append(sup[st.session_state.h]['VideoData'][str(i)][f'Video_Id{i}']['Likes'])
                Mcomments.append(sup[st.session_state.h]['VideoData'][str(i)][f'Video_Id{i}']['Comments'])


            # Inserting all Video details inside Video table
            for i in range(len(sup[st.session_state.h]['VideoData'])):
                cursor.execute("""
                INSERT IGNORE INTO video
                (playlistIDr,
                VideoTitle,
                VideoDesc,
                VideoID,
                PublishedDate,
                Views,
                Likes,
                Comments
                )
                VALUES (%s, %s, %s, %s, %s, %s,%s,%s)
                """, (playlist_id,Mvt[i],Mvd[i],Mvid[i],Mpd[i],Mviews[i],Mlikes[i],Mcomments[i]))

            # Inserting all Comments details inside Comment table
            for i in range(len(rass[0])):
                published_date = datetime.strptime(rass[0]['Published'][i], "%Y-%m-%dT%H:%M:%SZ").date()
                cursor.execute("""
                INSERT IGNORE INTO com
                (
                CommentID,
                VideoID,
                comments,
                author,
                Published_date
                )
                VALUES (%s, %s, %s, %s,%s)
                """, (rass[0]['CommentID'][i], rass[0]['VID'][i], rass[0]['Comments'][i], rass[0]['Author'][i], published_date))        

            st.success(f"Channel data {sup[str(st.session_state.h)]['ChannelData']['channel_name']} inserted successfully! Click the above button to proceed")
            del sup[st.session_state.h]
            

    # sk is just to pass as key inside the selectbox which keeps changing at each loop
    sk=0

    # Input from User to proceed 
    ny=['YES',"NO"]
    choice1 = st.selectbox('Can we move to Exporting data to MySQL', ny,index=1)
    
    # Creating session variables to get live updates
    if 'dub' not in st.session_state:
        st.session_state.dub = titleee
        st.session_state.win=''
        st.session_state.inox=0
        

    # Inputting the Channel to mitigate to MYSQL
    while  choice1=="YES":
        if len(st.session_state.dub)> 0 or choice=="YES":
            choice1='NO'
            st.header("Exporting Data to MySQL")
            st.write(st.session_state.dub)

            default_value = 'Select from the options'
            extended_options = [default_value] + st.session_state.dub
            choice2 = st.selectbox('Which one do you want to export', extended_options,key=sk+100)

            kol1,kol2,kol3,kol4,kol5 =st.columns(5)
            with kol5:
                next = st.checkbox(f'Move Next')
                if next:
                    flag+=1
                    choice1="No"
                    
            if choice2 == default_value:
                st.write('select from the options')

            else:
                inox = st.session_state.dub.index(choice2)
                st.session_state.win=choice2
                st.write(st.session_state.win)
                st.write('length of title:',len(st.session_state.dub))
                hmm = st.button(f'Export {st.session_state.win} to MySQL')
                if hmm:
                    mitigate(choice2)
            sk+=1
         
        else:
            choice1='NO'
            st.success('All Channels are Added')
            flag+=1




# EDA on Data stored in MYSQL
while flag>1:
    flag=2
    st.header("Retrieve data from the SQL database")
    st.info("""
    There are 3 Tables in MYSQL
    1)channel
    2)Video
    3)Com
    """)

    # Saving the channel table as Dataframe as QChan
    Tq = f"SELECT * FROM channel"
    cursor.execute(Tq)

    results = cursor.fetchall()

    QChan=[]
    for row in results:
        #Access the columns using the column index or name
        QChan.append(row[:])
    QChan=pd.DataFrame(QChan)
    Qcol = f"SHOW COLUMNS FROM channel"
    cursor.execute(Qcol)
    column_names = [column[0] for column in cursor.fetchall()]
    QChan.columns = column_names


    # Saving the video table as Dataframe as Qvid
    Qq = f"SELECT * FROM Video"
    cursor.execute(Qq)
    results1 = cursor.fetchall()

    Qvid=[]
    for row in results1:
        Qvid.append(row[:])
    Qvid=pd.DataFrame(Qvid)
    Qcol1 = f"SHOW COLUMNS FROM Video"
    cursor.execute(Qcol1)
    Qvid.columns = [column[0] for column in cursor.fetchall()]

    # Saving the comments table as Dataframe as Qcom
    Cq = f"SELECT * FROM Com"
    cursor.execute(Cq)
    results2 = cursor.fetchall()

    Qcom=[]
    for row in results2:
        Qcom.append(row[:])
    Qcom=pd.DataFrame(Qcom)
    Qcol2 = f"SHOW COLUMNS FROM Com"
    cursor.execute(Qcol2)
    Qcom.columns = [column[0] for column in cursor.fetchall()]

    # Displaying the Questions that can be answered
    st.write("""
    You Can Select the Number for getting the corresponding Questions Answered
    """)
    st.write("""
    1 ->What are the names of all the videos and their corresponding channels?
    """)
    st.write("""
    2 ->Which channels have the most number of videos, and how many videos do they have?
    """)
    st.write("""
    3 -What are the top 10 most viewed videos and their respective channels?
    """)
    st.write("""
    4 ->How many comments were made on each video, and what are their corresponding video names?
    """)
    st.write("""
    5 ->Which videos have the highest number of likes, and what are their corresponding channel names?
    """)
    st.write("""
    6 ->What is the total number of likes and dislikes for each video, and what are their corresponding video names?
    """)
    st.write("""
    7 ->What is the total number of views for each channel, and what are their corresponding channel names?
    """)
    st.write("""
    8 ->What are the names of all the channels that have published videos in the year 2022?
    """)
    st.write("""
    10 ->Which videos have the highest number of comments, and what are their corresponding channel names?
    """)

    # Input from user 
    n1 = st.number_input('Enter the number')
    if n1==1:
        query1 = """
            SELECT Video.VideoTitle, channel.channelName1
            FROM channel
            JOIN Video ON channel.playlistID= Video.playlistIDr
            """
        cursor.execute(query1)
        rows1 = cursor.fetchall()

        st.write('Names of all the videos and their corresponding channels shown Below:')
        for row in rows1:
            cmn= row[0]
            psr= row[1]
            st.write(f"{cmn}", '=', f"{psr}") 

    if n1==2:
        vt4=[]
        vc4=[]
        for i in range(len(QChan)):
            vt4.append(QChan.loc[i]['channelName1'])
            vc4.append(QChan.loc[i]['totVid'])
        jet = pd.DataFrame(list(zip(vt4,vc4)))
        jet.columns=['Channel_Name','Total Videos']
        Qject = jet.sort_values('Total Videos', ascending=False)
        st.write('Top 5 Channels based on their No.of Videos',Qject.head(5))


    if n1==3:
        query2 = """
            SELECT channel.channelName1,Video.VideoTitle, Video.Views
            FROM channel
            JOIN Video ON channel.playlistID= Video.playlistIDr
            """
        cursor.execute(query2)
        rows3 = cursor.fetchall()
        cnn=[]
        cvc=[]
        vvv=[]

        for row in rows3:
            cnn.append(row[0])
            cvc.append(row[1])
            vvv.append(row[2])
        combi1 = pd.DataFrame(list(zip(cnn,cvc,vvv)))
        combi1.columns=['Channel_Name','Video_Title','Video_Views']
        comb1 = combi1.sort_values('Video_Views', ascending=False)
        st.write('Top 5 Most Viewed Video with their Channel Name',comb1.head(5))

    if n1==4:
        vt1=[]
        cc1=[]
        st.subheader('Video Title Vs Comment Count')
        for i in range(len(Qvid)):
            vt1.append(Qvid.loc[i]['VideoTitle'])
            cc1.append(Qvid.loc[i]['Comments'])
        jet1= pd.DataFrame(list(zip(vt1,cc1)))
        jet1.columns=['Video Title','Comment Count']
        st.dataframe(jet1)

    if n1==5:  
        query3 = """
            SELECT Video.Likes, channel.channelName1,Video.VideoTitle
            FROM channel
            JOIN Video ON channel.playlistID= Video.playlistIDr
            """
        cursor.execute(query3)
        rows2 = cursor.fetchall()
        lk=[]
        cn=[]
        vc=[]
        for row in rows2:
            lk.append(row[0])
            cn.append(row[1])
            vc.append(row[2])   
        combi = pd.DataFrame(list(zip(cn,vc,lk)))
        combi.columns=['Channel_Name','Video_Title','Likes']
        comb = combi.sort_values('Likes', ascending=False)
        st.write('Top 5 Liked Video with their Channel Name',comb.head(5))

    if n1==6:  
        vt=[]
        vl=[]
        st.subheader('Video Title Vs Likes Count')
        for i in range(len(Qvid)):
            vt.append(Qvid.loc[i]['VideoTitle'])#'|||',
            vl.append(Qvid.loc[i]['Likes'])
        jet = pd.DataFrame(list(zip(vt,vl)))
        jet.columns=['VideoTitle','VideoLikes']
        st.dataframe(jet)
        st.error('Dislikes Disabled by YouTube as per Latest Guidelines')

    if n1==7:  
        tv1=[]
        cn1=[]
        for i in range(len(QChan)):
            tv1.append(QChan.loc[i]['views'])
            cn1.append(QChan.loc[i]['channelName1'])
        jet3 = pd.DataFrame(list(zip(cn1,tv1)))
        jet3.columns=['ChannelName','Total Views']
        st.dataframe(jet3)
        
    if n1==8:  
        query4 = """
            SELECT channel.channelName1,Video.VideoTitle, Video.PublishedDate
            FROM channel
            JOIN Video ON channel.playlistID= Video.playlistIDr
            """
        cursor.execute(query4)
        rows4 = cursor.fetchall()
        cn2=[]
        vt2=[]
        pb2=[]
        for row in rows4:
            cn2.append(row[0])
            vt2.append(row[1])
            pb2.append(row[2])
        combi3 = pd.DataFrame(list(zip(cn2,vt2,pb2)))       
        combi3.columns=['Channel_Name','VideoTitle','Video_Published_Date']    
        combi3['Video_Published_Date'] = pd.to_datetime(combi3['Video_Published_Date'])  # Convert to datetime type
        start_date = pd.to_datetime('2022-01-01')
        end_date = pd.to_datetime('2023-01-01')
        comb3 = combi3[(combi3['Video_Published_Date'] >= start_date) & (combi3['Video_Published_Date'] <= end_date)]
        st.dataframe(comb3)

    if n1==10:
        query5 = """
            SELECT channel.channelName1,Video.VideoTitle, Video.Comments
            FROM channel
            JOIN Video ON channel.playlistID= Video.playlistIDr
            """
        cursor.execute(query5)
        rows5 = cursor.fetchall()
        cn3=[]
        vt3=[]
        vc3=[]
        for row in rows5:
            cn3.append(row[0])
            vt3.append(row[1])
            vc3.append(row[2])
        combi4 = pd.DataFrame(list(zip(cn3,vt3,vc3)))       
        combi4.columns=['Channel_Name','VideoTitle','Comment Count']   
        comb4 = combi4.sort_values('Comment Count', ascending=False) 
        st.write('Top 5 Most Commented Video with their Channel Name',comb4.head(5))
    
    st.subheader('If you want to see some Plots with the data, click the button below:')
    quan=st.checkbox('View Plots')
    if quan:
        st.header('will see some charts and graphs')

        st.write('Comparing the Subscribers Count between the channels:')
        fig = plt.figure(figsize=(10, 4))
        sns.barplot(x='channelName1',y='subsCount',data=QChan)
        st.pyplot(fig)

        st.write('Comparing the Total Views between the channels:')
        fig1 = plt.figure(figsize=(10, 4))
        sns.barplot(x='channelName1',y='views',data=QChan)
        st.pyplot(fig1)

        st.write('Comparing the Total No.of Videos between the Channel:')
        cna=list(Qvid['VideoTitle'])
        fig2 = plt.figure(figsize=(10, 4))
        sns.barplot(x='channelName1',y='totVid',data=QChan)
        st.pyplot(fig2)

        st.write('Comparing the Total Views between the Videos:')
        cna=list(Qvid['VideoTitle'])
        fig3 = plt.figure(figsize=(10, 5))
        sns.barplot(x='Views',y='VideoTitle',data=Qvid)
        st.pyplot(fig3)

        st.write('Comparing the Total Likes between the Videos:')
        fig4 = plt.figure(figsize=(10, 5))
        sns.barplot(x='Likes',y='VideoTitle',data=Qvid)
        st.pyplot(fig4)

        Qvid['Date'] = pd.to_datetime(Qvid['PublishedDate'])
        Qvid['Month'] = Qvid['Date'].dt.month
        st.write('Comparing the published Month vs Views:')
        fig5 = plt.figure(figsize=(10, 5))
        sns.lineplot(x='Month',y='Views',data=Qvid)
        st.pyplot(fig5)

        Qvid['Date'] = pd.to_datetime(Qvid['PublishedDate'])
        Qvid['Month'] = Qvid['Date'].dt.month
        st.write('Comparing the published Month vs Likes:')
        fig6 = plt.figure(figsize=(10, 5))
        sns.lineplot(x='Month',y='Likes',data=Qvid)
        st.pyplot(fig6)

    flag-=1

    myconnection.commit()
    myconnection.close()
    mongo_client.close()
