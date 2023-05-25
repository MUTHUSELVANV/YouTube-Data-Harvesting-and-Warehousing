# YouTube-Data-Harvesting-and-Warehousing
The code allows users to enter maximum of 10 YouTube channel IDs on a Streamlit app, extract channel, video, and comment details, store them in MongoDB, transfer that stored data to MySQL, and perform EDA and plotting on the MySQL data.

The code follows a linear workflow with the following stages:

1) Extraction of YouTube data.
2) Storage of data in MongoDB as unstructured data.
3) Conversion of unstructured data to structured data by transferring it from MongoDB to MySQL.
4) User can insert details one by one while storing data in MySQL.
5) Perform exploratory data analysis on the MySQL data.
6) Plotting graphs based on the data.
