# data_engineering_project

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

This project consists of downloading soccer stats (Standings and all matches) of the major soccer leagues in the world through an [API](https://www.football-data.org).

The code is divided in 3 files:

    1. project_pipeline/api_call.py - creates a class __api_manager__ which has 2 methods. The first one extracts data from the api and filters out the data that is not useful. The second method consists of a static method which deletes all data from the designated file (this method is used to erase files from the temporary repositories fo data) 

    2. project_pipeline/snowflake_job.py - This file creates another class __snowflake_connector__ which does 3 main jobs.
        1. Upload and delete data from snowflake scenes (datalakes)
        2. Extract data from datalake and convert the json files to csv files
        3. Create and upload those csv files to a snowflake table

    3. main.py - file where the pipeline is ran.

## Project Organization

```
├── LICENSE            <- Open-source license if one is chosen
├── Makefile           <- Makefile with convenience commands like `make data` or `make train`
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── processed      <- Temporary storage for csv that were json before
│   └── raw            <- The original data in json format from the api
│
├── docs               <- A default mkdocs project; see www.mkdocs.org for details
│
├── pyproject.toml     <- Project configuration file with package metadata for 
│                         project pipeline and configuration for tools like black
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials.
│
├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures        <- Generated graphics and figures to be used in reporting
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment
│
└── project pipeline   <- Source code for use in this project.
|    │
|    ├── api_call.py             <- Requests to the api and data filtering
|    │
|    ├── snowflake_job.py        <- Set of jobs for snowflake usage, from uplaoding data to a stage to uploading
|    |                              transformed data to a table.
|    │
¨```

--------

