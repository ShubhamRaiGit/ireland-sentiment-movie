1) Query to retreive data

SELECT 
*
FROM 
(
SELECT 
ROW_NUMBER() OVER (ORDER BY VIEWCOUNT DESC) AS RN, 
Id,
PostTypeId,
AcceptedAnswerId,
ParentId,
CreationDate,
DeletionDate,
Score,
ViewCount,
OwnerUserId,
OwnerDisplayName,
LastEditorUserId,
LastEditorDisplayName,
LastEditDate,
LastActivityDate,
Title,
Tags,
AnswerCount,
CommentCount,
FavoriteCount,
ClosedDate,
CommunityOwnedDate,
ContentLicense
FROM POSTS WHERE VIEWCOUNT IS NOT NULL
) AS T
WHERE RN BETWEEN 200000 AND 200001


2) Start Hive

./start_hive.sh
bin/hiveserver2
bin/beeline -n hdoop -u jdbc:hive2://localhost:10000

3) Hive Query to create table and specify SERDE format for csv import
create table stackexchange(RN int, Id int, PostTypeId int,AcceptedAnswerId int,ParentId int,CreationDate timestamp,DeletionDate timestamp,Score BIGINT,ViewCount BIGINT,OwnerUserId varchar(255),OwnerDisplayName varchar(255),LastEditorUserId varchar(255),LastEditorDisplayName varchar(255),LastEditDate timestamp,LastActivityDate timestamp,Title timestamp,Tags varchar(255),AnswerCount int,CommentCount int,FavoriteCount int,ClosedDate timestamp,CommunityOwnedDate timestamp,ContentLicense varchar(255))  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

4) Put data on HDFS

hadoop fs -put <local_path> to <hdfs_location>

5) Load Data in Hive

load data inpath '/cas675/dataset/output_final.csv' into table stackexchange;

6) HIVE Queries

    a) select * from stackexchange_final order by viewcount desc limit 10
    output

    +-------------------------+-------------------------+---------------------------------+---------------------------------------+-------------------------------+-----------------------------------+-----------------------------------+----------------------------+--------------------------------+----------------------------------+---------------------------------------+---------------------------------------+--------------------------------------------+-----------------------------------+---------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------+-----------------------------------+------------------------------------+---------------------------------+-----------------------------------------+-------------------------------------+
| RN                      | Id                      | PostTypeId                      | AcceptedAnswerId                      | ParentId                      | CreationDate                      | DeletionDate                      | Score                      | ViewCount                      | OwnerUserId                      | OwnerDisplayName                      | LastEditorUserId                      | LastEditorDisplayName                      | LastEditDate                      | LastActivityDate                      | Title                                              | Tags                                               | AnswerCount                      | CommentCount                      | FavoriteCount                      | ClosedDate                      | CommunityOwnedDate                      | ContentLicense                      |
| 68717                   | 2951053                 | 1                               | 2951074                               |                               | 2010-06-01 15:30:10               |                                   | 35                         | 99999                          | 296446                           |                                       |                                       | user9934620                                | 2018-09-04 02:39:11               | 2018-09-04 02:39:11                   | How do I hide an HTML element before the page loads using jQuery | <jquery><html>                                     | 7                                | 0                                 | 15                                 |                                 |                                         | CC BY-SA 4.0                        |
| 68718                   | 38493564                | 1                               | 38493678                              |                               | 2016-07-21 01:49:02               |                                   | 46                         | 99999                          | 6468971                          |                                       |                                       |                                            |                                   | 2020-06-19 00:21:47                   | Chart area background color chartjs                | <javascript><jquery><canvas><html5-canvas><chart.js> | 2                                | 0                                 | 9                                  |                                 |                                         | CC BY-SA 3.0                        |
| 68716                   | 16440377                | 1                               |                                       |                               | 2013-05-08 12:12:07               |                                   | 63                         | 99999                          | 875016                           |                                       | 608639                                |                                            | 2018-04-28 11:05:15               | 2020-11-10 14:56:14                   | Replace whole line when match found with sed       | <shell><replace><sed><match>                       | 4                                | 2                                 | 16                                 |                                 |                                         | CC BY-SA 3.0                        |
| 68719                   | 17774768                | 1                               | 32695845                              |                               | 2013-07-21 17:06:52               |                                   | 38                         | 99998                          | 1900747                          |                                       |                                       |                                            |                                   | 2018-01-05 18:46:53                   | Python creating a shared variable between threads  | <python><multithreading>                           | 2                                | 4                                 | 8                                  |                                 |                                         | CC BY-SA 3.0                        |
| 68721                   | 12643792                | 1                               |                                       |                               | 2012-09-28 16:23:18               |                                   | 16                         | 99997                          | 999689                           |                                       | 3997521                               |                                            | 2021-03-30 08:25:01               | 2021-03-30 08:25:01                   | How to find parent tr of td in a table using jquery? | <jquery>                                           | 5                                | 2                                 | 5                                  |                                 |                                         | CC BY-SA 3.0                        |
| 68720                   | 25845836                | 1                               |                                       |                               | 2014-09-15 10:22:42               |                                   | 88                         | 99997                          | 2394556                          |                                       | -1                                    |                                            | 2017-04-13 12:13:47               | 2021-06-23 14:17:45                   | Could not obtain information about Windows NT group/user | <sql-server><sql-server-2008><sharepoint>          | 10                               | 0                                 | 23                                 |                                 |                                         | CC BY-SA 3.0                        |
| 68722                   | 5907575                 | 1                               |                                       |                               | 2011-05-06 06:25:49               |                                   | 187                        | 99996                          | 26143                            |                                       | 63550                                 |                                            | 2011-10-25 03:19:54               | 2020-07-11 15:06:40                   | How do I use pagination with Django class based generic ListViews? | <django>                                           | 3                                | 1                                 | 85                                 |                                 |                                         | CC BY-SA 3.0                        |
| 68723                   | 6061292                 | 1                               | 6292702                               |                               | 2011-05-19 15:44:19               |                                   | 39                         | 99996                          | 200987                           |                                       | 200987                                |                                            | 2019-06-30 10:58:04               | 2019-06-30 11:00:54                   | How can I pass complex objects as arguments to a RESTful service? | <json><parameter-passing><deserialization><cxf>    | 3                                | 0                                 | 18                                 |                                 |                                         | CC BY-SA 4.0                        |
| 68727                   | 41150975                | 1                               | 41151014                              |                               | 2016-12-14 19:48:08               |                                   | 40                         | 99995                          | 7041624                          |                                       | 5437621                               |                                            | 2019-04-29 10:54:47               | 2019-04-29 10:54:47                   | How to display list of running processes Python?   | <python><linux><centos>                            | 3                                | 1                                 | 14                                 |                                 |                                         | CC BY-SA 3.0                        |
+-------------------------+-------------------------+---------------------------------+---------------------------------------+-------------------------------+-----------------------------------+-----------------------------------+----------------------------+--------------------------------+----------------------------------+---------------------------------------+---------------------------------------+--------------------------------------------+-----------------------------------+---------------------------------------+----------------------------------------------------+----------------------------------------------------+----------------------------------+-----------------------------------+------------------------------------+---------------------------------+-----------------------------------------+-------------------------------------+