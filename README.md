# Rising Temperature's impact on US immigration and visitors

The scope of this capstone project for Udacity is to demonstrate a strong understanding of Spark and it's utilities by 
combining data from multiple sources into a data lake fashioned after a star schema. 

### _The main application, which exists in scripts/pyspark.py, is meant to be a step in some master node on some AWS EMR cluster._

### _The corresponding notebook, in scripts/pyspark_notebook.ipynb, can be run in a JupyterLab session for some AWS EMR cluster_

_Note: Since the dataset is too large to upload to Github, the dataset can be found 
[here](https://drive.google.com/open?id=1VN1hKfQF-UG5FbERMqRrJTvSqbQxDGLq). All accompanying scripts for the 
datasets will be in the scripts/ directory._


## Addressing certain scenarios

This code is meant to run as a step in an AWS EMR cluster with Spark. Given the plethora of configuration types, 
it is simple to address a few commonplace scenarios: 
1) The data is increased by 100x
    - Since all of the data is hosted on S3, availability should not be a problem; however, adjustments to the Spark 
    nodes will need to be made. Instead of using a general purpose m5.xlarge for all nodes, one could use a
    memory-optimized instance type for worker nodes, such as an r series instance, to allow each worker node
    to process more data at one time. 
2) The script should run on a daily basis, by 7am every day.
    - We can just as easily port this project to Airflow and create a DAG for submitting the PySpark job to AWS EMR.
    The script will need to be adjusted to extract data from the datasets from the current day to the previous day, and 
    thanks to Airflow's backfill capability, it will be trivial to implement such a change.
3) The Spark cluster needs to be accessed by 100+ people
    - Not only would we need more worker nodes since more requests will require more computational power, we can also
    increase the number of master nodes to increase availability of the cluster. 
    
## Decisions regarding the methodologies

### The tools and technologies

Using Spark seemed the best way to accomplish this project based on two major observations: 
1) There is little overlap between datasets in terms of their columns / keys, so although the data itself is in
a structured format, the datasets in relation to one another is unstructured. For example, there was no information 
regarding the month of arrival for asylum seekers; however, the US visitors dataset had such a column with valid values.
Putting all this information in a data lake seemed a better idea than a data warehouse given such unknown values.  
2) Some of these datasets are massive, with hundreds of thousands of rows and upwards of 53 columns to parse. 
The sheer act of copying all the data to a data warehouse would take serious time, so it made more sense to organize
the data in a memory-based structure. 

No matter which choice was the better one, some of the data was unusable in its current form. For example, Spark does 
not come with an excel parser natively and the excel files that needed to be parsed were not in a standard 
row-column format. Additional parsers were required to turn some of this unusable data into a form that Spark 
can understand without much of a headache (csv, json, and parquet as examples.) In the event that a dataset required
additional pruning / manual review of columns, you will find the corresponding parsers and rationales within that 
dataset's folder. 

### The schema

A star schema is one of the most popular and simplest style to implement in data warehouses; however, the data 
did not serve to be completely denormalized simply because of how the outputs are partitioned. For example, 
instead of using an id for each country in some dataframe, the country's name in the fact table should suffice if all 
dataframes that contain a 'country_name' or 'country_of_origin' column will be partitioned in parquet format
by the country-related column. Having a long id number instead would make it harder for queries in a data lake.

### The data model

To follow the star schema as closely as possible, the outputted data must reflect the given 
Spark schema accurately. 

- `temperatures`
    ``` 
    'avg_temperature': (IntegerType(), null=True, the average temperature),
    'avg_temperature_uncertainty': (IntegerType(), null=True, the uncertainty of the previous metric),
    'country': (StringType(), null=False, country name),
    'year': (IntegerType(), null=False, year as an int),
    'month': (IntegerType(), null=False, month as an int),
    'day': (IntegerType(), null=False, day as an int),
    'weekday': (StringType(), null=False, weekday abbreviation)
    ```
    - Partitioned by: country
    - Purpose: Store the historical land temperatures by country. 
  
- `asylum`
    ```
    'id': (LongType(), null=False, a monitonically increasing id for fact and time keeping),
    'country': (StringType(), null=False, country of origin),
    'num_arrivals': (IntegerType(), null=True, number of people who arrived),
    'num_accepted_affirmitavely': (IntegerType(), null=True, number accepted at the border),
    'num_accepted_defensively': (IntegerType(), null=True, number accepted after denied, then granted access by court)
    ``` 
    - Partitioned by: country
    - Purpose: Store how many people sought asylum by country, and how many were accepted, if the data exists for it. 
    - Note: This dataframe holds sum totals rather than individual records for asylum seekers.

- `visitor`
    ```
    'id': (IntegerType(), null=False, a montonically increasing id),
    'visa_category': (StringType(), null=False, a business, pleasure, or student visa),
    'visa_type': (StringType(), null=False, the type of visa issued),
    'port_of_entry_municipality': (StringType(), null=True, municipality of air, sea, or land port),
    'port_of_entry_region': (StringType(), null=True, region of air, sea, or land port),
    'country': (StringType(), null=False, country of origin),
    'visiting_state': (StringType(), null=False, US state visitng),
    ```
    - Partitioned by: country, visa_category
    - Purpose: Keeps records of every person who visited the US by country and by the nature of visitation.
    - Note: Sometimes the arrdate and depdate of the original dataset is null, therefore some of the 
    time data will be filled in with a default value (Jan, 1, 1960)

- `worker`
    ```
    'id': (IntegerType(), False, a montonically increasing id),
    'case_status': (T.StringType(), False, certified, withdrawn, denied, or certified-withdrawn),
    'visa_type': (T.StringType(), True, the type of visa issued),
    'employer_name': (T.StringType(), False, the name of the employer),
    'employer_city': (T.StringType(), False, the employer HQ city),
    'employer_state': (T.StringType(), False, the employer HQ state),
    'worksite_city': (T.StringType(), False, the city the worker is employed),
    'worksite_state': (T.StringType(), False, the state the worker is employed),
  
    ```
    - Partitioned by: visa_type
    - Purpose: Keep a record of all foreign legal workers
    - Note: Work-based immigration is the second most popular way to become a resident in the US, so it made sense to 
    target this bunch.
    
- `time`
    ```
    'id': (T.LongType(), False,),
    'immigration_type': (T.StringType(), False, will either be visitor, asylum, or worker),
    'arrival_year': (IntegerType(), null=False, year arrived as int),
    'arrival_month': (IntegerType(), null=True, month arrived as int),
    'arrival_day': (IntegerType(), null=True, day arrived as int),
    'arrival_weekday': (StringType(), null=True, weekday abbreviation),
    'expiry_year': (IntegerType(), null=True, year visa expires),
    'expiry_month': (IntegerType(), null=True, month visa expires),
    'expiry_day': (IntegerType(), null=True, day visa expires),
    'expiry_weekday': (StringType(), null=True, expiration day abbreviation)
    ``` 
    - Partition by: immigration_type, arrival_year
    - Purpose: Store the year of arrival for work visas, visitors, and asylum seekers, and other time data if it is 
    available.
    - Note: immigration_type points to one of the three immigrant dataframes illustrated above.
    - _IMPORTANT NOTE_: Think of id and immigration_type as a composite key, since there is a chance that a
    monitonically increasing id will share the same value among two or more dataframes with monitonically 
    increasing ids. This is also why the table is partitioned the way it is, to ensure there is no conflict
    among looking up a time record by its id. 
      
- `immigration_facts`
    ```
    'id': (LongType(), False, the id of the immigrant record),
    'country': (T.StringType(), True, the country name [unknown if immigration_type is worker]),
    'immigration_type': (T.StringType(), False, will either be visitor, asylum, or worker),
    'time_id': (T.LongType(), False, the id pointing to the entry in the time table)
    ```
    - Partition by: immigration_type
    - Purpose: Store a fact table that points to every other immigration dataframe in the data lake
    - Note: immigration_type points to one of the three immigrant dataframes illustrated above.
    - _IMPORTANT NOTE_: Think of id and immigration_type as a composite key, since there is a chance that a
    monitonically increasing id will share the same value among two or more dataframes with monitonically 
    increasing ids. This is also why the table is partitioned the way it is, to ensure there is no conflict
    among looking up a time record by its id. 
