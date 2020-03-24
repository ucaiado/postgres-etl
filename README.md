Data Modeling with Postgres
===========================

This project is part of the [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) program, from Udacity. I modeled user activity data for a music streaming app called Sparkify, where I define fact and dimension tables for a star schema and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

Currently, the startup doesn’t have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This data will be split in 5 tables, each one containing parts of the data from the logs files:

- users: Dimension table. Users in the app.
- songs: Dimension table. Songs in music database.
- artists: Dimension table. Artists in music database.
- time: Dimension table. Timestamps of records in songplays broken down into units.
- songplays: Fact table. Records in log data associated with song plays.

The database schema proposed would help them analyze the data they’ve been collecting on songs and user activity on their app using simple SQL queries on the tables


### Install
To set up your python environment to run the code in this repository, start by
 creating a new environment with Anaconda and install the dependencies.

```shell
$ conda create --name ngym36 python=3.6
$ source activate ngym36
$ pip install -r requirements.txt
```

You will also need to [configure](https://www.codementor.io/@engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb) Postgres to run the project in your local machine.

### Run
In a terminal or command window, navigate to the top-level project directory (that contains this README) and run the following command:

```shell
$ cd scripts/
$ . setup.sh
$ . include_data.sh
```

Then, go to the notebooks folder and run:

```shell
$ cd ../notebooks/
$ jupyter notebook test.ipynb
```

This will open the Jupyter Notebook software and the test notebook in your browser which you can use to explore the data included in the database.


### License
The contents of this repository are covered under the [MIT License](LICENSE).
