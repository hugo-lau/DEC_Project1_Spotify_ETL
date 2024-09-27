# Project plan

## Objective

The objective of our project is to provide analytical datasets from the Spotify API, focusing on new music releases and their associated audio features. This will facilitate deeper insights into music trends and preferences.


## Consumers

The primary users of our datasets are Data Analysts, Music Industry Professionals, and Marketers. They will access the data through a web interface or a dashboard that allows for easy exploration of trends and features related to new music releases.


## Questions

What questions are you trying to answer with your data? How will your data support your users?

Example:

> - What are the most popular new releases each week?
> - How do audio features (e.g., danceability, energy) correlate with track popularity?
> - What genres are most commonly released in new music?
> - How does the release frequency of artists vary over time?
> - Are there seasonal trends in new music releases?
> - How do the audio features of new releases compare across different artists?

## Source datasets

| Source Name           | Source Type | Source Documentation                       |
|----------------------|-------------|-------------------------------------------|
| Spotify New Releases  | REST API   | [Spotify API Documentation](https://developer.spotify.com/documentation/web-api/) |

The Spotify API updates with new releases daily, so we will schedule data extraction to occur at least once a day to capture all new music.

## Solution architecture

Following is the descriptive solution architecture diagram for implementing ETL on Spotify API

![images/Solution_Architecture.png](images/Solution_Architecture.png)

1. Python & Pandas was used for:
    1. Extracting the data about artists, songs, albums, new releases. Pipeline is set to run on a regular schedule
    2. Transforming data -> drop unnecessary columns, rename columns, and create a calendar (dates) dataframe that merges all the dates in 2023 and 2024 with the holidays information from the csv file.
    3. Load data to our postgres database.

2. PostgreSQL DBMS was used for storing all our data: artists, songs, ids

3. AWS RDS was used for hosting and managing our postgres database.

4. SQL was used for creating views off of the data that is loaded

5. Docker was used to containerize our pipeline

6. ECR was used to host our docker container

7. ECS was used to run the docker container

8. S3 was used to store the .env file.

## Breakdown of tasks

![project1_schedule](images/project1_schedule.png)
