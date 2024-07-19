"""
Module for simplifying data exploration with Spark DataFrames

Functions:
    imports()
        prints common import statements. Ex. "import pandas as pd"

Classes:
    DataSetLoader()
        loads datasets by name
    DataFrameSaver()
        saves/loads datasets to/from internal storage

DataFrame Methods:
    .prc()
        print count. Prints the number of rows in a comma separated format. Ex. 325,345,499
    .phead(n=5)
        print heading rows. Displays the first n rows
    .ps(n=5)
        print sample. Prints the count of rows and displays first n rows
    .pvc(["col_name", "another_col_name"])
        print values counts. Prints a table of row counts of combinations of unique values from the columns
    .ptr("col_name")
        print time range. Prints the earliest and the latest dates from the column
    .find_col("substring")
        find column. Prints columns which names contain the substring
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Hashable, List, Optional, Sequence, Union

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from IPython import get_ipython  # type: ignore
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

# set pandas data frame display properties
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.max_rows", 500)


def imports() -> None:
    imports_file_location = _get_data_file_path("imports.txt")
    with open(imports_file_location, "r") as f:
        print(f.read())


def _get_data_file_path(file_name: str) -> Path:
    return Path(__file__).resolve().parents[1] / "data" / file_name


def _get_global_variable(name: str) -> Any:
    """
    Get a variable with a provided name (if available) from a Databricks notebook at the runtime
    """
    return get_ipython().user_ns.get(name, None)


def _get_global_spark(spark_provided: Optional[SparkSession] = None) -> SparkSession:
    """
    Get spark session instance either from the provided variable
    or from the global variable that exists in jupyter notebook at runtime
    """
    spark = spark_provided if spark_provided is not None else _get_global_variable("spark")
    if not isinstance(spark, SparkSession):
        raise Exception("spark variable is not found/specified/wrong!")
    return spark


@dataclass
class _DataSetInfo:
    """Class for representing data for each dataset"""

    name: str
    path: str
    description: str = ""

    @property
    def dataframe_path(self) -> str:
        path, producer, container = 0, 1, 2
        parts = self.path.split("://")[1].split("/")
        return "/".join([f"abfss://{parts[producer]}@{parts[path]}"] + parts[container:-1]) + "/"


class DataSetLoader:
    """
    Class for checking the available datasets and loading them easily.
    Works based on a json configuration file with the required information

    Attributes
    ----------
    spark : SparkSession
        just pass the spark variable from the environment to make the thing work
    config_file_path : str
        path to json configuration file name if different from default

    Methods
    -------
    load(name: str):
        Loads dataset with the provided name if found in the configuration file
    show(verbose: bool = False)
        Prints information about the available datasets.
    """

    def __init__(self, spark: Optional[SparkSession] = None, config_file_path: Optional[str] = None):
        self.spark = _get_global_spark(spark)

        if config_file_path is None:
            config_file_location = _get_data_file_path("datasets_info.json")
        else:
            config_file_location = Path(config_file_path)
        self._data_info = self._load_data_sets_info(config_file_location)

    @staticmethod
    def _load_data_sets_info(file_path: Path) -> List[_DataSetInfo]:
        with open(file_path) as fp:
            data_sets_info = json.load(fp)
        return [_DataSetInfo(**ds_info) for ds_info in data_sets_info]

    def _load_as_format(self, path: str, format: str = "parquet") -> Optional[DataFrame]:
        """Tries to load a dataset in a given format. Returns None if can't"""
        try:
            return self.spark.read.format(format).load(path)
        except AnalysisException:
            return None

    def load(self, name: str) -> DataFrame:
        """Loads dataset with the provided name if found in the configuration file"""
        data_set_info = next((dsi for dsi in self._data_info if dsi.name.lower() == name.lower()), None)
        if data_set_info is None:
            raise Exception(
                "Dataframe with that name is nowhere to be found!\n"
                "Please run the DataSetLoader().show() command to get the list of all available datasets"
            )
        # trying to load the dataframe in different formats
        for format in ["parquet", "delta"]:
            sdf = self._load_as_format(data_set_info.dataframe_path, format)
            if sdf is not None:
                return sdf
        # when all the formats fail
        raise Exception("Could not load the dataframe! Probably due to an unknown format")

    def show(self, verbose: bool = False) -> None:
        """Prints information about the available datasets"""
        for n, ds in enumerate(self._data_info, start=1):
            print(f'{n:>2d}. "{ds.name}"')
            if verbose:
                print(f"    Description: {ds.description}")


class DataFrameSaver:
    """Class for saving and retrieving the intermediate data frames
    Attributes
    ----------
    spark : SparkSession
        just pass the spark variable from the environment to make the thing work
    data_path : str, default "dbfs:/mnt/teamdata/temp"
        path to the dataset storage folder
    prefix : str, default ""
        a string added to each dataframe name to ensure name uniqueness between the savers

    Methods
    -------
    save(sdf: DataFrame, name: str, mode: str = "overwrite"):
        Saves the data

    load(name: str):
        Loads the saved data

    show():
        Prints all the DataFrames from the path with the prefix if defined

    Usage
    -----
    # saving temporary files
    dfs = DataFrameSaver(spark, prefix="notebook_name_")
    dfs.save(sdf_some_data, "some_data")
    sdf_some_data = dfs.load("some_data")
    """

    def __init__(
        self, spark: Optional[SparkSession] = None, path: str = "dbfs:/mnt/teamdata/temp", prefix: str = ""
    ) -> None:
        self.spark = _get_global_spark(spark)
        self.data_path = path
        self.prefix = prefix

    def _full_name(self, name: str) -> str:
        return f"{self.prefix}{name}"

    def _full_path(self, name: str) -> str:
        full_name = self._full_name(name)
        return f"{self.data_path}/{full_name}"

    def load(self, name: str) -> DataFrame:
        return self.spark.read.format("delta").load(self._full_path(name))

    def save(self, sdf: DataFrame, name: str, mode: str = "overwrite") -> None:
        sdf.write.mode(mode).option("overwriteSchema", "True").saveAsTable(
            self._full_name(name), path=self._full_path(name)
        )

    def show(self) -> None:
        """Prints a list of the available datasets"""
        dbutils = _get_global_variable("dbutils")
        if dbutils is None:
            raise Exception("dbutils variable is not found/specified/wrong!")
        # get a list of file names that start from the prefix
        # though hide the prefix so the name can be used exactly in the load function
        file_names = [
            file_info.name
            # drop the trailing slash if exist
            .replace("/", "")
            # remove the prefix from the name to use the rest in the load function
            .replace(self.prefix, "")
            # for each file int the path
            for file_info in dbutils.fs.ls(self.data_path)
            # if that file starts with the prefix
            if file_info.name.startswith(self.prefix)
        ]
        # print out the available files
        for n, file_name in enumerate(file_names, start=1):
            print(f'{n:>2d}. "{file_name}"')


def prc(obj: Union[DataFrame, pd.DataFrame, pd.Series, int], title: str = "") -> None:
    """
    Print Row Count in a nice format

    Arguments
    ---------
    obj : spark DataFrame or pandas DataFrame or number
    title : String that precedes the number

    Example
    -------
    >>> prc(df, 'Row Count: ')
    Row Count: 234,555,032
    """
    if isinstance(obj, DataFrame):
        c = obj.count()
    elif isinstance(obj, pd.DataFrame) or isinstance(obj, pd.Series):
        c = len(obj.index)
    elif isinstance(obj, int):
        c = obj
    else:
        c = len(obj)
    print(f"{title}{c:,.0f}")


def _prc(self: DataFrame, title: str = "") -> None:
    """
    Print Row Count in a nice format

    Arguments
    ---------
    self : pyspark.sql.dataframe.DataFrame
    title : String that precedes the number

    Example
    -------
    >>> sdf.prc('Row Count: ')
    Row Count: 234,555,032
    """
    prc(self, title)


DataFrame.prc = _prc  # type: ignore
pd.core.frame.DataFrame.prc = _prc  # type: ignore


def _phead(self: Union[DataFrame, pd.DataFrame], n: int = 5) -> Union[pd.DataFrame, pd.Series]:
    """
    Take n rows of a spark dataframe and convert them to pandas dataframe

    Arguments
    ---------
    self : spark DataFrame or pandas DataFrame
    n : int, optional
        Number of rows to display

    Returns
    -------
    pandas DataFrame
    """

    if isinstance(self, DataFrame):
        return pd.DataFrame([_.asDict() for _ in self.take(n)]).reindex(self.columns, axis=1)

    if isinstance(self, pd.DataFrame):
        return self.head(n)

    raise TypeError("Unknown object passed. Don't know what to do with it")


DataFrame.phead = _phead  # type: ignore
pd.core.frame.DataFrame.phead = _phead  # type: ignore


def _ps(self: DataFrame, n: int = 2) -> pd.DataFrame:
    """
    Print sample for the dataframe.
        Prints row count and n row sample of the dataframe

    Arguments
    ---------
    self : pyspark.sql.dataframe.DataFrame
    n : int, optional
        Number of rows to display
    """
    self.prc()  # type: ignore
    return self.phead(n)  # type: ignore


DataFrame.ps = _ps  # type: ignore
pd.core.frame.DataFrame.ps = _ps  # type: ignore


def _value_counts(
    self: DataFrame,
    subset: Sequence[Hashable] = [],
    normalize: bool = False,
    sort: bool = True,
    ascending: bool = False,
    dropna: bool = True,
) -> DataFrame:
    """
    Return a Series containing counts of unique rows in the DataFrame.
    .. versionadded:: 1.1.0
    Parameters
    ----------
    self : pyspark.sql.DataFrame
    subset : single column name or a list of columns, optional
        Columns to use when counting unique combinations.
    normalize : bool, default False
        Return proportions as well as the counts.
    sort : bool, default True
        Sort by frequencies.
    ascending : bool, default False
        Sort in ascending order.
    dropna : bool, default True
        Don't include counts of rows that contain NA values.
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """

    if not bool(subset):
        subset = list(self.columns)

    cols = subset if isinstance(subset, list) else [subset]

    sdf = self.na.drop(subset=cols) if dropna else self

    counts = sdf.groupby(cols).count()

    if sort:
        counts = counts.orderBy("count", ascending=ascending)

    if normalize:
        counts = counts.withColumn("proportion", F.col("count") / F.sum("count").over(Window.partitionBy()))

    return counts


DataFrame.value_counts = _value_counts  # type: ignore


def _pvc(
    self: DataFrame,
    subset: Sequence[Hashable] = [],
    n: int = 5,
    count_col: str = "rows",
) -> pd.DataFrame:
    """
    Pandas dataframe with top n rows of values counts formatted with percentages

    Parameters
    ----------
    self : pyspark.sql.dataframe.DataFrame
    subset : single column name or a list of columns, optional
        Columns to use when counting unique combinations.
    n : int, optional, default 5
        Number of rows to display
    Returns
    -------
    pandas DataFrame
    """
    sdf = (
        self.value_counts(subset=subset, normalize=True, dropna=False)  # type: ignore
        .withColumn("rc", F.col("count"))
        .withColumnRenamed("count", count_col)
        .withColumn(count_col, F.format_number(F.col(count_col), 0))
        .withColumn("proportion", F.concat(F.format_number(100 * F.col("proportion"), 2), F.lit(" %")))
        .orderBy(F.desc("rc"))
        .drop("rc")
    )

    if n > 0:
        sdf = sdf.limit(n)

    return sdf.toPandas()


DataFrame.pvc = _pvc  # type: ignore


def _time_range(
    self: DataFrame,
    col: Optional[str] = None,
    to_date: bool = True,
    format: str = "yyyyMMdd",
) -> DataFrame:
    """
    Return a DataFrame with minimum and maximum time in readable format.
    Parameters
    ----------
    self : pyspark.sql.DataFrame
    col : column name, string
        Column with timestamps.
    to_date : a flag that says to convert the datetime to date for easier reading, boolean
    format : format of the time string (ignored for other types) 'yyyy-MM-dd HH:mm:ss', string
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """

    N_DIGITS_UNIX_TIME = 10

    # guessing the time column if not provided
    # 1. by column type
    col = col or next((c for c in self.columns if self.schema[c].dataType in [T.TimestampType(), T.DateType()]), None)
    # 2. by column name
    for uneducated_guess in ["time", "date"]:
        col = col or next((c for c in self.columns if uneducated_guess in c.lower()), None)

    if col is None:
        raise Exception("Time column is not specified!")

    col_type = self.schema[col].dataType

    if col_type == T.StringType():
        self = self.withColumn(col, F.to_timestamp(F.col(col).substr(1, len(format)), format))

    # assume unix timestamp format
    if col_type in [T.IntegerType(), T.LongType()]:
        self = (
            self
            # convert to string
            .withColumn(col, F.col(col).cast(T.StringType()))
            # take the first 10 digits to syncronise between formats in seconds and milliseconds
            .withColumn(col, F.col(col).substr(1, N_DIGITS_UNIX_TIME))
            # convert back to a number
            .withColumn(col, F.col(col).cast(T.LongType()))
            # convert to datetime
            .withColumn(col, F.from_unixtime(F.col(col)))
        )

    if to_date:
        self = self.withColumn(col, F.to_date(col))

    return self.agg(F.min(col), F.max(col))


DataFrame.time_range = _time_range  # type: ignore


def _ptr(
    self: DataFrame,
    col: Optional[str] = None,
    to_date: bool = True,
    format: str = "yyyyMMdd",
) -> None:
    """
    Prints minimum and maximum time in a readable format.
    Parameters
    ----------
    self : pyspark.sql.DataFrame
    col : column name, string
        Column with timestamps.
    to_date : a flag that says to convert the datetime to date for easier reading, boolean
    format : format of the time string (ignored for other types) 'yyyy-MM-dd HH:mm:ss', string
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """

    df = _time_range(self, col, to_date=to_date, format=format)
    col = df.columns[0][4:-1]

    res = df.collect()[0]
    print(f"{col}:")
    print(f"  min = {res[0]}")
    print(f"  max = {res[1]}")


DataFrame.ptr = _ptr  # type: ignore


def _find_col(self: Union[DataFrame, pd.DataFrame], substring: str, case_sensitive: bool = False) -> List[str]:
    """
    Return DataFrame columns that contain a substring.
    Parameters
    ----------
    self : pyspark.sql.DataFrame
    substring : str
        Part of column name to search for
    case_sensitive : bool, default False
        Account for letter case.
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """

    if case_sensitive:
        return [c for c in self.columns if substring in c]
    return [c for c in self.columns if substring.lower() in c.lower()]


DataFrame.find_col = _find_col  # type: ignore
pd.core.frame.DataFrame.find_col = _find_col  # type: ignore


def format_number_columns(
    df: pd.DataFrame, cols: List[str], precision: int = 0, as_pctg: bool = False
) -> pd.DataFrame:
    """
    Formats the number X to a format like '#,-#,-#.-', rounded to "precision" decimal places,
    and returns the result as a string.
    Use to prettify the summary tables.

    Arguments
    ---------
    df : pandas dataframe with numeric columns
    cols : str or list[str]
        The column name(s) of the numeric values to be formatted
    precision : int, default 0
        The number of decimal places
    as_pctg: bool, default False
        Represent proportion as a percentage

        Example:
        >>> df.pipe(format_number_columns, ['col1', 'col2'], 2)
          col1  |  col2  | col3
        ------------------------
           10.22|1,002.40| 50
    """
    result = df.copy()
    template = f"{{:,.{precision}f}}"
    multiplier = 1
    if as_pctg:
        template += " %"
        multiplier = 100
    if isinstance(cols, str):
        cols = [cols]
    for col in cols:
        result[col] = multiplier * df[col]
        result[col] = result[col].map(template.format)
    return result
