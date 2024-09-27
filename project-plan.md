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
    2. Transforming data -> drop unnecessary columns, rename columns.
    3. Load data to our postgres database.

2. PostgreSQL DBMS was used for storing all our data: artists, songs, ids

3. AWS RDS was used for hosting and managing our postgres database.

4. SQL was used for creating views off of the data that is loaded

5. Docker was used to containerize our pipeline

6. ECR was used to host our docker container

7. ECS was used to run the docker container

8. S3 was used to store the .env file.

## Breakdown of tasks

# Project Task List

## Phase 1 - Project Planning

| Task                                  | Status   | Start Date | Due Date  | Assigned to          |
|---------------------------------------|----------|------------|-----------|----------------------|
| Define Dataset                        |          | 09-26-2024 | 09-27-2024| Anyone               |
| Setup Git Collaboration               |          | 09-26-2024 | 09-27-2024| Anyone               |
| • Team members verify Git pull, branch, request works | | 09-26-2024 | 09-27-2024| Anyone |
| Complete Project Plan Template        |          | 09-26-2024 | 09-27-2024| Veda Bommareddy       |
| • Submit Draft Project Schedule       |          | 09-26-2024 | 09-26-2024| Hugo Lau              |
| • Review project plan template        |          | 09-26-2024 | 09-27-2024| Rob Casey, Hugo Lau   |

## Phase 2 - MVP (Extract, Transform, Load, Docker)

| Task                                  | Status   | Start Date | Due Date  | Assigned to          |
|---------------------------------------|----------|------------|-----------|----------------------|
| Extract Data                          |          | 09-26-2024 | 09-28-2024| Anyone               |
| • Static Extraction                   |          |            |           | Anyone               |
| • Live Extraction                     |          |            |           | Anyone               |
| • Full Extract                        |          | 09-26-2024 | 09-28-2024| Anyone               |
| • Incremental Extract                 |          | 09-26-2024 | 09-28-2024| Anyone               |
| Transform Data                        |          | 09-28-2024 | 09-30-2024| Anyone               |
| • 3 Transformation Techniques         |          | 09-28-2024 | 09-30-2024| Anyone               |
| • 5 Transformation Techniques         |          | 09-28-2024 | 09-30-2024| Anyone               |
| • 7 Transformation Techniques         |          | 09-28-2024 | 09-30-2024| Anyone               |
| Load Data                             |          | 09-28-2024 | 10-01-2024| Anyone               |
| • Load Data → Full Load               |          | 09-30-2024 | 10-01-2024| Anyone               |
| • Load Data → Incremental Load        |          | 09-30-2024 | 10-01-2024| Anyone               |
| • Load Data → Upsert Load             |          | 09-30-2024 | 10-01-2024| Anyone               |

## Phase 3 - Build Docker Image and Run on AWS

| Task                                  | Status   | Start Date | Due Date  | Assigned to          |
|---------------------------------------|----------|------------|-----------|----------------------|
| Build and Run Docker Locally          |          | 10-01-2024 | 10-01-2024| Anyone               |
| • Build a docker image using Dockerfile |          | 10-01-2024 | 10-01-2024| Anyone               |
| • Docker container runs locally       |          | 10-01-2024 | 10-01-2024| Anyone               |
| Deploy Docker Container to AWS        |          | 10-01-2024 | 10-03-2024| Anyone               |
| • Relational Database Service (RDS) - screenshot of dataset in target storage | | 10-01-2024 | 10-03-2024 | Anyone |
| • Elastic Container Registry (ECR) - screenshot of image in ECR | | 10-01-2024 | 10-03-2024 | Anyone |
| • S3 for 'env' file - screenshot of 'env' file in S3 | | 10-01-2024 | 10-03-2024 | Anyone |
| • IAM Role - screenshot of created role | | 10-01-2024 | 10-03-2024 | Anyone |

## Phase 4 - Testing/Logging, Documentation/Presentation

| Task                                  | Status   | Start Date | Due Date  | Assigned to          |
|---------------------------------------|----------|------------|-----------|----------------------|
| Write pipeline metadata logs to a database table | | 10-03-2024 | 10-04-2024 | Anyone |
| Implement unit tests using PyTest or similar | | 10-03-2024 | 10-05-2024 | Anyone |
| Documentation                         |          | 10-03-2024 | 10-07-2024| Anyone               |
| • Code documentation using Python docstrings and comments where reasonable | | 10-03-2024 | 10-07-2024 | Anyone |
| • Markdown documentation explaining the project context, architecture, installation/running instructions | | 10-03-2024 | 10-07-2024 | Anyone |
| Presentation                          |          | 10-07-2024 | 10-07-2024| Anyone               |
| • Project context and goals           |          | 10-07-2024 | 10-07-2024| Anyone               |
| • Datasets selected                   |          | 10-07-2024 | 10-07-2024| Anyone               |
| • Solution architecture diagram using draw.io or similar | | 10-07-2024 | 10-07-2024 | Anyone |
| • ELT/ETL techniques applied          |          | 10-07-2024 | 10-07-2024| Anyone               |
| • Final dataset and demo run (if possible) |       | 10-07-2024 | 10-07-2024| Anyone               |
| • Lessons learnt                      |          | 10-07-2024 | 10-07-2024| Anyone               |


![project1_schedule](images/project1_schedule.png)
