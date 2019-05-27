## Summary

This ETL pipeline was created for the music streaming app, Sparkify, to allow their teams to analyse the songs that their users are listening to. The decision was made to move processing and data storage into the cloud using AWS, as the app has grown in popularity and scale.

Sparkify data exists in the form of JSON log data, profiling user activity, and JSON metadata, describing the songs and artists that are being listened to. 

The ETL pipeline extracts these JSON files from Amazon S3 buckets, where they currently reside, and loads them into two staging tables in Amazon Redshift. From there, the data are transformed and loaded into a set of five analytics tables, one fact, and four dimension tables, that make it possible for the Sparkify analytics team to query the data.


## Operating Instructions

To run the ETL pipeline, follow these steps;

1. Ensure the Sparkify Redshift cluster is running and status is 'healthy'
2. Ensure that the relevant security policies and IAM role have been provisioned to allow the Redshift cluster to pull data from Sparkify S3 buckets
3. Copy credentials, IAM role ARN, and cluster endpoint into the dwh.cfg file
4. At the command line, run 'python create_tables.py' to create the staging and analytics tables
5. At the command line, run 'python etl.py' to load the staging tables, transform the data, and finally load into the analytics tables from the staging tables
6. Use the AWS Management Console's Redshift Query Editor to query the populated analytics tables


## ETL Justification

### Staging Tables

Staging tables are used to load data from Amazon S3 buckets into Redshift. They hold the data in its 'raw' format before any transformation has taken place.

#### staging_events

This table holds data from the JSON logs of user activity on the Sparkify app.

The table has the distkey set to the `artist` column - this is to allow later joining of both the staging_events and staging_songs tables on the `artist`/`artist_name` columns to create the songplays table - having both of these columns distributed together in the database will improve the efficiency of the join.

#### staging_songs

This table holds JSON-formatted metadata on the songs and artists listened to by users of the Sparkify app.

The table has the distkey set to the `artist_name` column - this is to allow later joining of both the staging_events and staging_songs tables on the `artist`/`artist_name` columns to create the songplays table - having both of these columns distributed together in the database will improve the efficiency of the join.


### Analytics Tables

Analytics tables are created using transformations performed on the staging tables.

#### songplays - fact table

This table is created through the joining of both of the staging tables, on their respective `artist`/`artist_name` columns, to bring together data about the song listened to , the user, and the user session, via foreign keys, into one fact table.

The table has `level` as a distribution key, as this is likely to be a key filtering column when performing analytics queries, with analysts probably interested in either paying or free users at any one time.

The table has `song_id` and `artist_id` as sorting keys, as it is also likely that queries may search for plays of particular songs or artists, and so this may help to speed up these queries.


#### users - dimension table

This table is created using data from the staging_events table.

The table has `gender` as a distribution key, as this is likely to be fairly evenly split in the dataset, as the data grows over time, and so provides acceptable distribution among the cluster node slices, allowing for parallel processing.


#### songs - dimension table

This table is created using data from the staging_songs table.

The table has `artist_id` as a distribution key, as it is likely that artists will have multiple songs in the database, and so doing this will keep those rows of data together across cluster node slices, allowing for more efficient queries.

The table has `year` as a sort key, as this is likely to be a key measure that queries will filter rows on.

#### artists - dimension table

This table is created using data from the staging_songs table.

The table has `location` as a distribution key, as this is likely to be fairly evenly split in the dataset, as the data grows over time, and so provides acceptable distribution among the cluster node slices, allowing for parallel processing.


#### time - dimension table

This table is created using the `ts` column from the staging_events table.

The table has `year` as a distribution key, as this is likely to be fairly evenly split in the dataset, as the data grows over time, and so provides acceptable distribution among the cluster node slices, allowing for parallel processing.