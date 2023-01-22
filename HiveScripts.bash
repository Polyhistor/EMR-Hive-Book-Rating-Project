# Downloading  and unzipping the data set
wget https://video.udacity-data.com/topher/2020/October/5f8f4412_bx-csv-data/bx-csv-data.zip;

unzip bx-csv-data.zip

# Place data inside HDFS 
hdfs dfs -put BX-CSV-Data /user/csv-data

# Creating a new database
CREATE DATABASE book_db;

USE book_db;

# Creating the necessary tables
CREATE EXTERNAL TABLE IF NOT EXISTS bxUsers(
    UserID string,
    Location string,
    Age int
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' 
LOCATION '/user/csv-data'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS bxBooks(
    ISBN int,
    BookTitle string,
    BookAuthor string,
    YearOfPublication int,
    Publisher string, 
    ImageURLS string, 
    ImageURLM string,
    ImageURLL string
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' 
LOCATION '/user/csv-data'
tblproperties ("skip.header.line.count"="1");


