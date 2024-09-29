# DE-bootcamp-project1
This repository contains the ETL process on Spotify Web API


# Setting up the enironment
## Virtual Environment (conda)
1. Open the terminal and create a conda environment
```
conda create -n project1 python=3.9
```

2. Activate the conda env
```
conda activate project1
```

3. Install the requirements in project1 conda environment
```
pip install -r requirements.txt
```

4. entry point (ps. make sure you are in the etl_project path before running)
'''
python -m pipelines.spotify.py
'''


