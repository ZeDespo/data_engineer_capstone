from datetime import date, timedelta
from typing import Dict, Tuple, List, Optional, Any
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.functions import udf  # Necessary for decorators

import argparse


def create_spark_session(app_name: str) -> SparkSession:
    """
    Creates the spark session on the master node.
    :param app_name: The name of the app
    :return: A SparkSession Object
    """
    conf = SparkConf().setAppName(app_name)
    return SparkSession.builder.config(conf=conf).getOrCreate()


def data_quality_checks(df: DataFrame, col: str, schema: Dict[str, Tuple[str, Any]]) -> None:
    """
    Ensure that the dataframe not only has data inside of it, but all the values that belong to a column
    reside in that column. The latter is due to the number of unions in this application, so it's possible that
    a bad union between two dataframes could lead to values that should belong to id mistakenly end up in
    a date column, for example.
    :param df: The dataframe to check.
    :param col: The column to search for to check for dataframe size.
    :param schema: The schema to compare the order of the dataframe's column values.
    :return: Nothing.
    """
    assert df.select(col).limit(1).count() > 0
    try:
        assert df.columns == list(schema)
    except AssertionError:
        err = "Dataframe with column structure of {} does not match expected column structure {}." \
            .format(df.columns, list(schema))
        raise AssertionError(err)


def extract_time_and_fact_vals(spark: SparkSession, df: DataFrame, i_type: str,
                               time_df: DataFrame or None, fact_df: DataFrame or None) -> (DataFrame, DataFrame):
    """
    Pulling from applicable dataframes, extract the appropriate values for the time and fact dataframes.
    Please note, the reason both of these dataframes are filled with one function is to avoid redundancy,
    given the simplicity of filling both dataframes.
    :param spark: The SparkSession object
    :param df: The dataframe to extract values from
    :param i_type: The immigration type (asylum, worker, visitor)
    :param time_df: The time dataframe to populate
    :param fact_df: The fact dataframe to populate.
    :return: The time and fact dataframes after getting key values extracted from the passed dataframe.
    """
    time_schema, fact_schema = get_schema('time'), get_schema('fact')
    if not time_df:
        time_df = make_empty_df(spark, time_schema)
    if not fact_df:
        fact_df = make_empty_df(spark, fact_schema)
    time_fields = ['id', 'immigration_type', 'arrival_year', 'arrival_month', 'arrival_day', 'arrival_weekday',
                   'expiry_year', 'expiry_month', 'expiry_day', 'expiry_weekday']
    df = df.withColumn('immigration_type', F.lit(i_type))
    if i_type == 'asylum':
        for_time = _fill_missing_columns(df.selectExpr('id', 'immigration_type', 'year as arrival_year'), time_df)
        for_fact = df.selectExpr('id', 'country', 'immigration_type', 'id as time_id')
    elif i_type == 'visitor':
        for_time = df.select(time_fields)
        for_fact = df.selectExpr('id', 'country', 'immigration_type', 'id as time_id')
    elif i_type == 'worker':
        df = df.withColumn('country', F.lit('Unknown').cast(T.StringType()))
        for_time = df.select(time_fields)
        for_fact = df.selectExpr('id', 'country', 'immigration_type', 'id as time_id')
    else:
        raise ValueError("{} not a vaid immigration type".format(i_type))
    return time_df.union(for_time.select(list(time_schema))), fact_df.union(for_fact.select(list(fact_schema)))


def get_schema(key: str) -> Dict[str, Tuple[Any, bool]]:
    """
    Function's sole purpose is to hold the schemas of the various dataframes and return the specified one
    :param key: The key to the type of schema we want
    :return: A schema for a dataframe.
    """
    schemas = {
        'asylum': {
            'id': (T.LongType(), False,),
            'country': (T.StringType(), False,),
            'year': (T.LongType(), False,),
            'num_arrivals': (T.IntegerType(), True,),
            'num_accepted_affirmitavely': (T.IntegerType(), True,),
            'num_accepted_defensively': (T.IntegerType(), True,)
        },
        'country': {
            'avg_temperature': (T.IntegerType(), True,),
            'avg_temperature_uncertainty': (T.IntegerType(), True,),
            'country': (T.StringType(), False,),
            'year': (T.IntegerType(), False,),
            'month': (T.IntegerType(), False,),
            'day': (T.IntegerType(), False,),
            'weekday': (T.StringType(), False,),
        },
        'fact': {
            'id': (T.LongType(), False),
            'country': (T.StringType(), True,),
            'immigration_type': (T.StringType(), False,),
            'time_id': (T.LongType(), False,)
        },
        'time': {
            'id': (T.LongType(), False,),
            'immigration_type': (T.StringType(), False,),  # will either be visitor, asylum, or worker
            'arrival_year': (T.IntegerType(), False,),
            'arrival_month': (T.IntegerType(), True,),
            'arrival_day': (T.IntegerType(), True,),
            'arrival_weekday': (T.IntegerType(), True,),
            'expiry_year': (T.IntegerType(), True,),
            'expiry_month': (T.IntegerType(), True,),
            'expiry_day': (T.IntegerType(), True,),
            'expiry_weekday': (T.IntegerType(), True,),
        },
        'visitor': {
            'id': (T.IntegerType(), False,),
            'visa_category': (T.StringType(), False,),
            'visa_type': (T.StringType(), False,),
            'port_of_entry_municipality': (T.StringType(), True,),
            'port_of_entry_region': (T.StringType(), True,),
            'country': (T.StringType(), False,),
            'visiting_state': (T.StringType(), False,),
            'arrival_year': (T.IntegerType(), True,),
            'arrival_month': (T.IntegerType(), True,),
            'arrival_day': (T.IntegerType(), True,),
            'arrival_weekday': (T.StringType(), True,),
            'expiry_year': (T.IntegerType(), True,),
            'expiry_month': (T.IntegerType(), True,),
            'expiry_day': (T.IntegerType(), True,),
            'expiry_weekday': (T.StringType(), True,)
        },
        'worker': {
            'id': (T.IntegerType(), False,),
            'case_status': (T.StringType(), False,),
            'visa_type': (T.StringType(), True,),
            'employer_name': (T.StringType(), False,),
            'employer_city': (T.StringType(), False,),
            'employer_state': (T.StringType(), False,),
            'worksite_city': (T.StringType(), False,),
            'worksite_state': (T.StringType(), False,),
            'arrival_year': (T.IntegerType(), False,),
            'arrival_month': (T.IntegerType(), True,),
            'arrival_day': (T.IntegerType(), True,),
            'arrival_weekday': (T.StringType(), True,),
            'expiry_year': (T.IntegerType(), True,),
            'expiry_month': (T.IntegerType(), True,),
            'expiry_day': (T.IntegerType(), True,),
            'expiry_weekday': (T.StringType(), True,)
        }
    }
    return schemas[key]


def make_empty_df(spark: SparkSession, columns: Dict[str, Tuple[Any, bool]]) -> DataFrame:
    """
    Create an empty Spark dataframe with a given schema.
    :param spark: The SparkSession object
    :param columns: A dictionary with the key being the column name, value[0] being the type, value[1] if it's null
    :return: An empty dataframe with some schema
    """
    schema = T.StructType([T.StructField(key, columns[key][0], columns[key][1]) for key in columns])
    return spark.createDataFrame([], schema)


def parse_asylum_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Parse the asylum seeker data to the appropriate schema.
    :param spark: the SparkSession object
    :param input_path: location of the data
    :return: A Spark dataframe
    """
    df = spark.read.csv(input_path + "refugee_and_migrant_data/*.csv", header=True) \
        .dropDuplicates() \
        .withColumn('id', F.monotonically_increasing_id())
    df = _clean_string_column(df, 'country')
    schema = get_schema('asylum')
    asylum_df = make_empty_df(spark, schema).union(df.select(list(schema)))
    return asylum_df


def parse_country_climate_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Parse the asylum seeker data to the appropriate schema.
    :param spark: the SparkSession object
    :param input_path: location of the data
    :return: A Spark dataframe
    """
    cast = ['dt', 'AverageTemperature as avg_temperature',
            'AverageTemperatureUncertainty as avg_temperature_uncertainty', 'Country as country']
    df = spark.read.csv(input_path + "climate_data/*.csv", header=True) \
        .selectExpr(*cast) \
        .dropDuplicates()
    df = df.withColumn('date', F.to_date('dt')) \
        .withColumn('year', F.year('date')) \
        .withColumn('month', F.month('date')) \
        .withColumn('day', F.dayofmonth('date')) \
        .withColumn('weekday', F.date_format('date', 'E')) \
        .drop('date', 'dt')
    df = _clean_string_column(df, 'country')
    schema = get_schema('country')
    country_df = make_empty_df(spark, schema).union(df.select(list(schema)))
    return country_df


def parse_visitor_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Parse the asylum seeker data to the appropriate schema.
    :param spark: the SparkSession object
    :param input_path: location of the data
    :return: A Spark dataframe
    """
    cit_and_res = spark.read.json(input_path + "i94_visitor_data/i94cit_and_i94res.json", multiLine=True)
    port_of_entry = spark.read.json(input_path + "i94_visitor_data/i94port.json", multiLine=True)
    visa_type = spark.read.json(input_path + "i94_visitor_data/i94visa.json", multiLine=True)
    to_filter = ['cicid as id', 'i94res', 'i94port', 'arrdate', 'i94visa', 'i94addr as visiting_state',
                 'depdate', 'visatype as visa_type']
    df = spark.read.parquet(input_path + "i94_visitor_data/sas_data/*.parquet").selectExpr(*to_filter).dropDuplicates()
    req_lookup = {
        'i94res': ['region as country'],
        'i94port': ['municipality as port_of_entry_municipality', 'region as port_of_entry_region'],
        'i94visa': ['type as visa_category']
    }
    for name, dtype in df.dtypes:
        if dtype == 'double':
            df = df.withColumn(name, df[name].cast(T.IntegerType()))
        if name in req_lookup:
            if name == 'i94res':  # Look up the code to one of the external dataframes
                args = (cit_and_res, df, req_lookup[name], name,)
            elif name == 'i94port':
                args = (port_of_entry, df, req_lookup[name], name,)
            elif name == 'i94visa':
                args = (visa_type, df, req_lookup[name], name,)
            else:
                raise ValueError("Cannot process {}".format(name))
            df = _code_lookup(*args)
        elif name == 'arrdate' or name == 'depdate':  # Number of days since 1/1/1960
            prefix = 'arrival' if name == 'arrdate' else 'expiry'
            d = "{}_date".format(prefix)
            df = df.withColumn(d, _convert_to_date(df[name])) \
                .withColumn('{}_year'.format(prefix), F.year(d)) \
                .withColumn('{}_month'.format(prefix), F.month(d)) \
                .withColumn('{}_day'.format(prefix), F.dayofmonth(d)) \
                .withColumn('{}_weekday'.format(prefix), F.date_format(d, 'E')) \
                .drop(d, name)
    for column in ['port_of_entry_municipality', 'country', 'visa_category', 'arrival_weekday', 'expiry_weekday']:
        df = _clean_string_column(df, column)
    schema = get_schema('visitor')
    visitor_df = make_empty_df(spark, schema).union(df.select(list(schema)))
    return visitor_df


def parse_worker_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Parse the asylum seeker data to the appropriate schema.
    :param spark: the SparkSession object
    :param input_path: location of the data
    :return: A Spark dataframe
    """
    csv, to_filter = "h1b_kaggle.csv", ["CASE_STATUS", "EMPLOYER_NAME", "YEAR", "WORKSITE"]
    df1 = spark.read.csv(input_path + "legal_immigrant_data/{}".format(csv), header=True) \
        .selectExpr(*_lower_case_headers(to_filter)) \
        .dropDuplicates() \
        .withColumn("visa_class", F.lit("H-1B"))
    df1 = df1.withColumn('split', F.split(df1['worksite'], ',')) \
        .withColumn("worksite_city", F.col('split')[0]) \
        .withColumn("worksite_state", F.col('split')[1]) \
        .drop("split", "worksite")
    df1 = df1.withColumn('worksite_state', _abbreviate_state(df1.worksite_state))
    csv = "H-1B_Disclosure_Data_FY17.csv"
    to_filter = ['CASE_STATUS', 'VISA_CLASS', 'EMPLOYMENT_START_DATE', 'EMPLOYMENT_END_DATE', 'EMPLOYER_NAME',
                 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'WORKSITE_CITY', 'WORKSITE_STATE']
    df2 = spark.read.csv(input_path + "legal_immigrant_data/{}".format(csv), header=True) \
        .selectExpr(*_lower_case_headers(to_filter)) \
        .dropDuplicates()
    states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY',
        'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND',
        'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }
    valid_row_allignment = lambda x: (F.length(x) == 2) & (x.isin(states))
    df2 = df2.filter(valid_row_allignment(df2.worksite_state)) # Mini data quality check to check row allignment
    for d in ['start_date', 'end_date']:
        prefix = 'arrival' if d == 'start_date' else 'expiry'
        column = 'employment_start_date' if d == 'start_date' else 'employment_end_date'
        df2 = df2.withColumn(d, F.to_date(column)) \
            .withColumn('{}_year'.format(prefix), F.year(d)) \
            .withColumn('{}_month'.format(prefix), F.month(d)) \
            .withColumn('{}_day'.format(prefix), F.dayofmonth(d)) \
            .withColumn('{}_weekday'.format(prefix), F.date_format(d, 'E')) \
            .drop(d, column)
    new_df = _fill_missing_columns(df1, df2)
    new_df = new_df.union(df2).dropDuplicates().withColumn('id', F.monotonically_increasing_id())
    new_df = new_df.withColumnRenamed('visa_class', 'visa_type')
    for column in ['case_status', 'employer_name', 'worksite_city', 'arrival_weekday', 'expiry_weekday']:
        new_df = _clean_string_column(new_df, column)
    schema = get_schema('worker')
    worker_df = make_empty_df(spark, schema).union(new_df.select(list(schema)))
    return worker_df


def write(df: DataFrame, output_path: str,
          partition_by: Optional[List[str]] = None,
          mode: Optional[str] = 'overwrite',
          format: Optional[str] = 'parquet') -> None:
    """
    Save the dataframe to some storage (s3, HDFS, etc) AFTER a data quality check
    :param df: The dataframe to write
    :param output_path: where to write it
    :param partition_by: how to partition the data (only valid with parquet format)
    :param mode: Overwrite, add, etc
    :param format: write as a parquet, json, csv, etc
    :return:
    """
    if partition_by:
        df.write.partitionBy(partition_by).format(format).mode(mode).save(output_path)
    else:
        df.write.format(format).mode(mode).save(output_path)


@udf(T.StringType())
def _abbreviate_state(state: str) -> str:
    """
    Helper function to the worker parser that abbreviates a US state
    :param state: the name of the state
    :return: either the symbol or the original value of state if no valid key is found.
    """
    states = {
        'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA', 'colorado': 'CO',
        'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
        'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA',
        'maine': 'ME', 'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
        'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH',
        'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND',
        'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI',
        'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT', 'vermont': 'VT',
        'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
        'district of columbia': 'DC'
    }
    if state:
        s = state[1:].lower()  # each state in the dataframe is prepended with a space thanks to the split function.
        return state if s not in states else states[s]


def _clean_string_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Standardize the characters in a StringType() column. Lower case them, then replace spaces with '_'
    :param df: The dataframe to perform the operation on.
    :param column_name: The column to perform the operation on.
    :return: The same dataframe with the formatted column.
    """
    return df.withColumn(column_name, F.lower(F.regexp_replace(column_name, ' ', '_')))


def _code_lookup(lookup: DataFrame, main_df: DataFrame, to_select: List[str], main_df_code_col: str):
    """
    Helper function for parsing visitor data. The visitor dataframe has numeric codes that need to be looked up by
    several other dataframes.
    :param lookup: The dataframe that some value corresponds to
    :param main_df: The dataframe the value was found in
    :param to_select: The columns to select from the lookup dataframe
    :param main_df_code_col: The column containing the codes to reference in the lookup dataframe
    :return: lookup and main_df joined together at the specific column.
    """
    if 'code' not in to_select:
        to_select.append('code')
    return lookup.selectExpr(*to_select) \
        .join(main_df, lookup.code == main_df[main_df_code_col]) \
        .drop('code', main_df_code_col)


@udf(T.DateType())
def _convert_to_date(d: int) -> date:
    """
    Convert dates from a numeric value (number of days since 1/1/1960) to a Date format
    :param d: Number of days since 1/1/1960
    :return: A date object.
    """
    if not d:
        d = 0
    return date(1960, 1, 1) + timedelta(days=d)


def _fill_missing_columns(df_with_missing_cols: DataFrame, df: DataFrame, default: Optional[Any] = None) -> DataFrame:
    """
    To perform a union, one needs to have two dataframes with the same columns. This function will fill add on the
    columns as needed.
    :param df_with_missing_cols: The smaller data frame
    :param df: The dataframe with the missing columns the first one needs
    :param default: The default value to fill for the missing values.
    :return: The smaller dataframe with the added columns.
    """
    if default:
        if type(default) is int:
            data_type = T.IntegerType
        elif type(default) is str:
            data_type = T.StringType
        elif type(default) is float:
            data_type = T.DoubleType
        else:
            raise ValueError("Type {} is not supported.".format(type(default)))
    else:
        data_type = T.StringType
    cols = set(df_with_missing_cols.columns)
    new_df = df_with_missing_cols
    for i in df.columns:
        if i not in cols:
            new_df = new_df.withColumn(i, F.lit(default).cast(data_type()))
    return new_df


def _lower_case_headers(columns: List[str]) -> List[str]:
    """
    Lower the capitalization of the headers in some dataframe.
    :param columns: The columns to lower.
    :return: A list to pass into a selectExpr statement
    """
    fields = []
    for name in columns:
        if name == 'YEAR':
            change_to = 'arrival_year'
        else:
            change_to = name.lower()
        fields.append('{} as {}'.format(name, change_to))
    return fields


def main(input_path: str, output_path: str) -> None:
    """
    NOTE: THIS CODE IS INTENDED TO RUN ON SOME MASTER NODE IN AWS EMR!

    Organize some data lake meant to find a correlation between immigration / travel records
    and rising global temperatures.
    :param: input_path: Where the dataset is. If S3, optimized for reads
    :param: output_path: Where the output will be. If S3, optimized for writes.
    :return:
    """
    spark = create_spark_session('capstone')
    country_df = parse_country_climate_data(spark, input_path)
    asylum_df = parse_asylum_data(spark, input_path)
    visitor_df = parse_visitor_data(spark, input_path)
    worker_df = parse_worker_data(spark, input_path)
    time_df, fact_df = None, None
    for df, i_type in [(asylum_df, 'asylum',), (visitor_df, 'visitor',), (worker_df, 'worker',)]:
        time_df, fact_df = extract_time_and_fact_vals(spark, df, i_type, time_df, fact_df)
    to_check = [
        (country_df, 'country', [],),
        (asylum_df, 'asylum', ['year'],),
        (visitor_df, 'visitor',
         ['arrival_year', 'arrival_month', 'arrival_day', 'arrival_weekday', 'expiry_year', 'expiry_month',
          'expiry_day', 'expiry_weekday'],),
        (worker_df, 'worker',
         ['arrival_year', 'arrival_month', 'arrival_day', 'arrival_weekday', 'expiry_year', 'expiry_month',
          'expiry_day', 'expiry_weekday'],),
        (time_df, 'time', [],),
        (fact_df, 'fact', [],)
    ]
    write_args = [
        ('{}temperatures/'.format(output_path), ['country'],),
        ('{}asylum/'.format(output_path), ['country'],),
        ('{}visitors/'.format(output_path), ['country', 'visa_category'],),
        ('{}workers/'.format(output_path), ['visa_type'],),
        ('{}time/'.format(output_path), ['immigration_type', 'arrival_year'],),
        ('{}immigration_facts/'.format(output_path), ['immigration_type'],)
    ]
    for i, (df, key, to_drop) in enumerate(to_check):  # Only write if data quality has been approved.
        col = 'id' if df is not country_df else 'country'
        data_quality_checks(df, col, get_schema(key))
        if to_drop:
            df = df.drop(*to_drop)
        write(df, *write_args[i])


parser = argparse.ArgumentParser(description='Identify the path that holds the data/ folder.')
parser.add_argument('--path', default='s3://date-engineering-capstone/')
path = parser.parse_args()
inp, out = path + "data/", path + "output/"
if 's3://' in inp:
    inp = inp.replace('s3://', 's3a://')
if 's3a://' in out:
    out = out.replace('s3a://', 's3://')
main(inp, out)
