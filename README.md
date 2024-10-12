# DEC - Project 1

This was my bootcamp project aims to create a pipeline solution that extract from a live dataset that periodically updates, and load it to a relational database, to form an analytical database to help answer questions about music trends. The different containizered ELT packages will be deployed on AWS cloud.

## Objective

The focus will be running the multiple extractions needed to gather correlations between new music releases, the audio features and popularity.

## Consumers
The primary users of our datasets are Data Analysts, Music Industry Professionals, and Marketers. They will access the data through a web interface or a dashboard that allows for easy exploration of trends and features related to new music releases.

## Questions

What questions are you trying to answer with your data? How will your data support your users?

Example:
> - What are the most popular new releases each week?
> - How do audio features (e.g., danceability, energy) correlate with track popularity?
> - How do the audio features of new releases compare across different artists?

## Source datasets

| Source Name           | Source Type | Source Documentation                       |
|----------------------|-------------|-------------------------------------------|
| Spotify New Releases  | REST API   | [Spotify API Documentation](https://developer.spotify.com/documentation/web-api/) |

The spotify API uses a RESTFUL API. Four API calls were used:
> - Get new releases – identify latest releases
> - Get album tracks – get tracks inside each released album
> - Get track audio features – fetch audio features associated with each track
> - Get track details – get details of track including popularity

## Solution architecture

Following is a high-level solution architecture diagram for implementing ETL on Spotify API.
The solution will deploy two containers, one supporting a python pipeline and another supporting sql.

![images/Solution_Architecture.png](images/Solution_Architecture.png)

1. Python, Jinja, Pandas was used for:
    1. Extracting the data about artists, songs, albums, new releases. 
    2. Transforming data -> drop unnecessary columns, rename columns, sorting data.
    3. Load data to our postgres database.

2. PostgreSQL DBMS was used for storing all our data: artists, songs, ids

3. AWS RDS was used for hosting and managing our postgres database.

4. SQL was used to query and create reports off the datasets loaded in PostgresQL. 

5. Docker was used to containerize our pipeline

6. ECR was used to host our docker container

7. ECS was used to run the docker container

8. S3 was used to store the .env file.

## ELT/ELT Techniques Applied
#Initial ER Diagram of the database
![images/Spotify_ERD.png](images/Spotify_ERD.png)

**Update Oct 12, 2024**
Based on feedback from the presentation, the following two key changes were made:
>- Incremental extract based on album_id instead of track_id. 
Checking on track_id, led a situation where application was running more code than necessarily, and doing more unnecessarily API callls, esp given Spotify API rate limit.
The general assumption is that albums don't change, and if they do change, they will count as a "new release".
>- as a learning, we explored both python and sql pipelines. Taking lessons learned, the code was streamlined into one pipeline, where python would handle the incremental extract and load into a postgresql database, while sql would handle the transform.
This worked much better as python was better equipped to handle the nested data structures that spotify while transforms were a lot more effortless with sql transform.



To explore and apply the techniques learned in the lessons, two container images were created. 
One was a python pipeline, that did was a full extract and served as our MVP. 
The other was a sql peipeline, which did an incremental extract.

**Extraction**
For both pipelines, the extraction breakdown was as follow:
1. Get new releases - identify the latest albums released and obtain the album id.
2. Get album tracks - from the album id, get the latest tracks and associated track_id inside each release album.
3. Get audio features - from the track_id, get the audio features associated with each track.
4. Get track details - from the track_id, get the track details including popularity.

**Incremental Extraction** 


A notable challenge, was that Spotify did not offer an API endpoint that supported timestamps of the releases on a given date. The API endpoint in particularly only supported an offset to get the X(X=number of determined) previous releases.
We landed on two notable solutions:

1) We did an incremental extraction by comparing the track_ids extracted from the API to list of track_ids that were already loaded into the database. Track_IDs that were not previously loaded to database, the pipeline would proceed to run two additional API calls for each track, to gather the audio features and track details. See attached screenshot for the code to accomplish this

Code in Extraction Function to support incremental extraction
```python 
# Filter to find only new IDs
new_ids = [id_ for id_ in source_ids if id_ not in existing_ids]  # Find new IDs
new_ids = [f"'{id_}'" for id_ in new_ids]  # Quote the string IDs
```
SQL Template
```sql
{% if config["extract_type"] == "incremental" %}
WHERE
    id IN ({{ new_ids | join(', ') }})  -- Ensure new_ids are properly formatted
{% else %}
```
2) Because the API endpoint, supported an offset to get X = number of latest releases, a timestamp was inserted to prove the incremental extract worked, and didn't simply overlap previous extractions.

3) The nature of data coming from Spotify APi consist of a lot of nested data structures and list of dictionaries. As a result, prior to loading or transforming the data, information had to be extracted from these certain datasets.

Sample code to obtain artist name for this list of dictionaries.
```python
df_track_popularity['album.artists'] = df_track_popularity['album.artists'].apply(lambda x: x[0]['name'] if x else None)
````
**Load**

To delimit data coming from two different pipelines, python and sql pipeline each had their own databases. From SQL pipeline perspective, the python database function as the "source" database for its extraction purposes.

**Transform**

For both pipeline, the tables for audio features and track details were merged. Afterwards, certain columns selected, and re-ordered based on certain criterias such as artist name, song, release date and popularity.
There were more transformative opportunities to provide more data insights especially with SQL Templates.


## Limitations and Lessons Learned
Here are some of the challenges and lessons encountered during the project:
> - **O.Auth2.0** - Spotify APi uses O-Auth2.0 authentication, which involves a two-step process where using client credentials, Spotify authorization server provides an access token which expires in 60 mins. As best practice, the refresh token should be retrieved to avoid exposing clients secret key but due to technical debt, we did not obtain the refresh token. The access token is used for doing the API calls.

Encode Client Credentials into base-64
```python
auth_string = f"{client_id}:{client_secret}"
auth_bytes = auth_string.encode("utf-8")
auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")
````

> - **Incremental Extraction** - due to the nature of the API endpoint, the offset of latest releases on a periodically updated live dataset, meant we needed to find a way to find a solution for incremental extraction and proved that it work. The later was easily solved by inserting a timestamp. For incremental, finding a column anchor was difficult as release dates and timestamp was not feasible. Ultimately, we had to find a way to compare the track_ids to determined which ones needed to be extracted. 
> - **Data Schema** - the nexted data structure and list of dictionaries meant more exploration and work was required to extract information from data columns. 
> - **API Rate Limit** - related to the extraction, we encountered a few 429 rate limits, due to our pipeline solution. This made incremental extraction important to limit the number of API calls on track information.
> - **Git Branch Conflicts** - about a third through the project, it was learned to push changes from respective branches to avoid merge conflicts but if these scenarios occured, using the rebase line changes, was an effective way to solve conflicts
> - **Python vs SQL Pipelines** In exploring the two pipeline solutions, a few things were discovered
1. Python was the much better choice in doing the extraction and loading, especially in dealing with the data structure of Spotify API. However, due to python transformation being in memory. It's not scalable in doing multiple transformations.
2. SQL was much easier and better for transforming information, allowing us to quickly do queries of key information and transform them accordingly, making multiple transformations at once. However, extractions were a lot more difficult esp with the nature of Spotify data.


# Setting up the enironment

## Cloning the project
```
> git init
> git clone https://github.com/vedaBommareddy23/DE-bootcamp-project1.git
> git pull
```

## Virtual Environment (conda)
1. Open the terminal and create a conda environment
```
> conda create -n project1 python=3.9
```

2. Activate the conda env
```
> conda activate project1
```

3. Install the requirements in project1 conda environment
```
> pip install -r requirements.txt
```

4. entry point for pythonetl 
```
> cd app
> python -m etl_project.pipelines.spotify
```

5. entry point for sqletl
```
> cd app2
> python extract_load.py
```

6. docker build and run
```
 -- Container#1 - python-etl (full-extract)
> docker build --platform=linux/amd64 -t project1_pythonetl .
> docker run --env-file .env project1_pythonetl:latest

 -- Container#2 - sql-etl(incremental extract)
> docker build --platform=linux/amd64 -t project1_sqletl .
> docker run --env-file .env project1_ project1_sqletl:latest
```
## AWS Screenshots

**Dataset loaded in RDS**

![project1_schedule](images/Loaded_to_RDS_Database.jpg)

**Scheduled Task in ECS**
![project1_schedule](images/ECS_Screenshot_of_scheduled_task_in_ECS.jpg)

**Container Image in ECR**
![project1_schedule](images/ECR_Image_Screenshot.jpg)

**IAM Created Role**
![project1_schedule](images/IAM_Role_Created.jpg)

S3 Bucket containing env file
![project1_schedule](images/S3_Bucket_hosting_Environable_File.jpg)


