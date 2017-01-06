# arra_analysis
Analysis for the ARRA Instagram Project

This project is designed to get our researchers going with analysis of the ARRA data.  It is a python package which will contain core functions for accessing the data.

## Getting Started

create a file in the top-level directory called `.db` and fill it with a 1-line DB connection string, ie:

`postgres://username:password@db_url:5432/db_name`

then you can access this module from your python script just like usual with `import` protocol:

`import arradata`

get the dataset like this:

`dataset = arradata.data(refresh = True)` 

leave out the `refresh` kwarg after the first time to save time and access the saved local copy.

`dataset = arradata.data()`

Get raw connection to DB:

`conn = arradata.get_connection()`

## The Data

The data will be delivered as a pandas dataframe.  All columns that come from the survey data will be prefixed with the the work `survey_`.  All other columns are data derived from the Instagram data.
