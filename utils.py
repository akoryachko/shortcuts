import pandas as pd
import re
from typing import Dict, Tuple, List, Optional, Union, Sequence, Hashable
try:
    import pyspark
except ImportError:
    import findspark
    findspark.init(spark_home='/opt/spark-2.3.0/')
    import pyspark
import pyspark.sql.functions as F  # noqa: F401
from pyspark.sql.window import Window  # noqa: F401
from pyspark.sql.dataframe import DataFrame  # noqa: F401
from pyspark.sql.column import Column


pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', 500)

def prc(obj, title: Optional[str] = ''):
    """ Print Row Count in a nice format

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
    elif isinstance(obj, pd.DataFrame):
        c = len(obj.index)
    elif isinstance(obj, int):
        c = obj
    else:
        c = len(obj)
    print(f'{title}{c:,.0f}')


def _prc(self: DataFrame, title: Optional[str] = ''):
    """ Print Row Count in a nice format

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

DataFrame.prc = _prc
pd.core.frame.DataFrame.prc = _prc


def prn(df: pd.DataFrame, cols: List[str], precision: int = 0, as_pctg: bool = False):
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
        >>> df.pipe(prn, ['col1', 'col2'], 2)
          col1  |  col2  | col3
        ------------------------
           10.22|1,002.40| 50
    """
    result = df.copy()
    template = f'{{:,.{precision}f}}'
    multiplier = 1
    if as_pctg:
        template += ' %'
        multiplier = 100
    if isinstance(cols, str):
        cols = [cols]
    for col in cols:
        result[col] = multiplier*df[col]
        result[col] = result[col].map(template.format)
    return result


def _phead(self: DataFrame, n: Optional[int] = 5) -> pd.DataFrame:
    """ Take n rows of a spark dataframe and convert them to pandas dataframe

    Arguments
    ---------
    self : pyspark.sql.dataframe.DataFrame
    n : int, optional
        Number of rows to display

    Returns
    -------
    pandas DataFrame

    """
    return (
        pd.DataFrame([_.asDict() for _ in self.take(n)])
        .reindex(self.columns, axis=1)
    )

DataFrame.phead = _phead


def _ps(self: DataFrame, n: Optional[int] = 2):
    """ Print sample for the dataframe.
        Prints row count and n row sample of the dataframe

    Arguments
    ---------
    self : pyspark.sql.dataframe.DataFrame
    n : int, optional
        Number of rows to display

    """
    self.prc()
    display(self.phead(n))

DataFrame.ps = _ps
pd.core.frame.DataFrame.ps = _ps


def _save_sdf(
        self: DataFrame,
        path: Optional[str] = 'cache',
        delete_at_exit: Optional[bool] = False,
        mode: Optional[str] = "overwrite",
        ) -> DataFrame:
    """
    Persists a Spark DataFrame in Parquet format with optional cleanup when the process
    running the SparkContext exits.

    Reads the DataFrame back immediately after writing it in order to truncate the Spark
    job graph.

    Parameters
    ----------
    self : pyspark.sql.DataFrame or pyspark.sql.DataFrameWriter
        The dataframe to be represented by the parquet dataset or its writer
    path : str
        The path to the parquet dataset to be written or read from
    delete_at_exit : bool, default False
        If True, attempt to delete the path at Python exit, assuming the user still has permission
        to do so (e.g., Kerberos credentials are valid)
    mode : str, default "overwrite"
        Options: "append", "overwrite", "ignore", "error"

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """

    return pts.curried.parquet_dataframe(path=path,
                                         df=self,
                                         mode=mode,
                                         delete_at_exit=delete_at_exit)

DataFrame.save_sdf = _save_sdf


def _value_counts(
        self,
        subset: Sequence[Hashable] = None,
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
        Don’t include counts of rows that contain NA values.
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """

    if subset is None:
        subset = list(self.columns)

    cols = subset if isinstance(subset, list) else [subset]

    sdf = self

    if dropna:
        sdf = sdf.na.drop(subset=cols)

    counts = sdf.groupby(cols).count()

    if sort:
        counts = counts.orderBy('count', ascending=ascending)

    if normalize:
        counts = (
            counts
            .withColumn(
                'proportion',
                F.col('count')/F.sum('count').over(Window.partitionBy())
            )
        )

    return counts

DataFrame.value_counts = _value_counts


def _pvc(
        self,
        subset: Sequence[Hashable] = None,
        n: Optional[int] = 5,
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
        self
        .value_counts(subset=subset, normalize=True, dropna=False)
        .withColumn('rc', F.col('count'))
        .withColumnRenamed('count', 'rows')
        .withColumn('rows', F.format_number(F.col('rows'), 0))
        .withColumn('proportion', F.concat(F.format_number(100*F.col('proportion'), 2), F.lit(' %')))
        .orderBy(F.desc('rc'))
        .drop('rc')
    )

    if n > 0:
        sdf = sdf.limit(n)

    return sdf.toPandas()

DataFrame.pvc = _pvc


def _time_range(
        self,
        col: str = None,
        ) -> DataFrame:
    """
    Return a DataFrame with minimum and maximum time in readable format.
    Parameters
    ----------
    self : pyspark.sql.DataFrame
    col : column name, optional
        Column with timesstamps. If None, the first column that ends with "Ts" is used
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """

    if not col:
        col = next(c for c in self.columns if c.endswith('Ts'))

    return (
        self
        .withColumn(col, F.from_unixtime(F.col(col) / 1000))
        .agg(F.min(col), F.max(col))
    )
DataFrame.time_range = _time_range


def _ptr(
        self,
        col: str = None,
        ) -> DataFrame:
    """
    Prints minimum and maximum time in a readable format.
    Parameters
    ----------
    self : pyspark.sql.DataFrame
    col : column name, optional
        Column with timesstamps. If None, the first column that ends with "Ts" is used
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
    """

    df = self.time_range(col)
    col = df.columns[0][4:-1]

    res = df.collect()[0]
    print(f'{col}:')
    print(f'  min = {res[0]}')
    print(f'  max = {res[1]}')

DataFrame.ptr = _ptr


def _find_col(
        self,
        substring: str,
        case_sensitive: bool = False,
        ) -> DataFrame:
    """
    Return DataFrame columns that contain sunstring.
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

DataFrame.find_col = _find_col