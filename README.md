# DE-bootcamp-project1
This repository contains the ETL process on Spotify Web API


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