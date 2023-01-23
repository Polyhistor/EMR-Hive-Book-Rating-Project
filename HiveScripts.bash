# --- PART 1 

# Downloading and unzipping the data set
wget https://video.udacity-data.com/topher/2020/October/5f8f4412_bx-csv-data/bx-csv-data.zip;

unzip bx-csv-data.zip

# Place data inside HDFS 
hdfs dfs -put BX-CSV-Data/BX-Book-Ratings.csv /user/csv-data/BX-Book-Ratings.csv
hdfs dfs -put BX-CSV-Data/BX-Books.csv /user/csv-data/BX-Books.csv
hdfs dfs -put BX-CSV-Data/BX-Users.csv /user/csv-data/BX-Users.csv

hive 

# Creating a new database
CREATE DATABASE book_db;

USE book_db;

# Creating the necessary tables
CREATE EXTERNAL TABLE IF NOT EXISTS bxUsers(
    UserID string,
    Location string,
    Age int
) 
ROW FORMAT 
DELIMITED FIELDS 
TERMINATED BY '|' 
STORED AS TEXTFILE
LOCATION '/user/csv-data'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS bxBooks(
    isbn String,
    title String,
    author String,
    year_of_publication String,
    publisher String,
    image_url_s String,
    image_url_m String,
    image_url_l String
) 
ROW FORMAT 
DELIMITED FIELDS 
TERMINATED BY '|'
STORED AS TEXTFILE 
LOCATION '/user/csv-data'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS bxBookRatings(
    UserID int,
    ISBN int,
    rating int
) 
ROW FORMAT 
DELIMITED FIELDS 
TERMINATED BY '|'
STORED AS TEXTFILE 
LOCATION '/user/csv-data'
tblproperties ("skip.header.line.count"="1");

# Running th necessary queries for business cases
SELECT COUNT(DISTINCT userid) 
FROM book_db.bx_users 
WHERE age IS NOT NULL or age != "NULL";

SELECT COUNT(*) 
FROM book_db.bx_users
WHERE age >= 21 AND age <= 30;

books they have published. */
SELECT author, COUNT(title) as total_books_published 
FROM book_db.bx_books
GROUP BY author 
ORDER BY total_books_published DESC 
LIMIT 5;

# --- PART 2

# Downloading  and unzipping the data set
wget https://video.udacity-data.com/topher/2020/November/5fabec84_game-reviews.tsv/game-reviews.tsv.gz;

gzip -d game-reviews.tsv.gz 

hdfs dfs -mkdir /user/csv-data/

hdfs dfs -put game-reviews.tsv /user/csv-data/game-reviews.tsv

hive 

# Creating a new database
CREATE DATABASE demodb;

CREATE EXTERNAL TABLE gameReviews(
    marketplace string, 
    customer_id string,
    review_id string, 
    product_id string, 
    product_parent string, 
    product_title string, 
    product_category string, 
    star_rating int, 
    helpful_votes int, 
    total_votes int,
    vine string, 
    verified_purchase string, 
    review_headline string,
    review_body string, 
    review_date string
) 
ROW FORMAT 
DELIMITED FIELDS 
TERMINATED BY '|'
STORED AS TEXTFILE 
LOCATION '/user/csv-data'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE gameReviewsPartitioned(
    marketplace string, 
    customer_id string,
    review_id string, 
    product_id string, 
    product_parent string, 
    product_title string, 
    product_category string, 
    star_rating int, 
    helpful_votes int, 
    total_votes int,
    vine string, 
    verified_purchase string, 
    review_headline string,
    review_body string, 
    review_date string
) 
PARTITIONED BY (year string)
ROW FORMAT 
DELIMITED FIELDS 
TERMINATED BY '|'
STORED AS TEXTFILE 
LOCATION '/user/csv-data'
tblproperties ("skip.header.line.count"="1");

SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE gameReviewsPartitioned 
PARTITION (year)
SELECT *, year(review_date) from gameReviews;
