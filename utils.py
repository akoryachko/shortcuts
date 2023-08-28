import pandas as pd
from typing import Dict, List, Optional, Sequence, Hashable, Any
try:
    import pyspark
except ImportError:
    import findspark
    findspark.init(spark_home='/opt/spark-2.3.0/')
    import pyspark
import pyspark.sql.functions as F  # noqa: F401
from pyspark.sql.window import Window  # noqa: F401
from pyspark.sql.dataframe import DataFrame  # noqa: F401
from collections import namedtuple
import math


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
        result[col] = multiplier * df[col]
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

    return self.curried.parquet_dataframe(path=path,
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
                F.col('count') / F.sum('count').over(Window.partitionBy())
            )
        )

    return counts


DataFrame.value_counts = _value_counts


def _pvc(
        self,
        subset: Sequence[Hashable] = None,
        n: Optional[int] = 5,
        count_col='rows',
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
        .withColumnRenamed('count', count_col)
        .withColumn(count_col, F.format_number(F.col(count_col), 0))
        .withColumn('proportion', F.concat(F.format_number(100 * F.col('proportion'), 2), F.lit(' %')))
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
        case_sensitive: bool = False
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


MDField = namedtuple(
    "MDField",
    ("path", "post_process", "is_required"),
    defaults=([], None, False),
)


def map_dictionary(in_dict: Dict[str, Any], map: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a dictionary produced from the fields of another dictionary
    with optional modifications and a check for required field

        Parameters:
            in_dict: a source or input dictionary
            map: a dictionary that defines a map from the input dictionary to the output dictionary

        Returns:
            out_dict: the resulting dictionary

        Examples of the input parameters and the result are as follows:

            in_dict = {
                'path': {
                    'through': {
                        'nested': {
                            'structures': 'post_process_me'
                        }
                    }
                }
                'input_dict_top_level_field': True,
                'list_key_in_dict': [
                    {
                        'path_within_the_list_dict': 'u_need_me'
                    },
                    {
                        'path_within_the_list_dict': 'u_need_me_too'
                    }
                ]
                'absolute_path_within_the_in_dict': 42
            }

            map = {
                'k1': "A constant",
                'k2': MDField(['path', 'through', 'nested', 'structures'], post_process_function),
                'k3': {
                    'kd1': MDField(['input_dict_top_level_field']),
                    'kd2': [
                        'list_key_in_dict',  # LIST_NAME
                        # LIST DICT
                        {
                            'kdl1': MDField(['path_within_the_list_dict'], is_required=True),
                            'kdl2': MDField(['absolute_path_within_the_in_dict'])
                        }
                    ]
                }
            }

            out_dict = {
                'k1': "A constant",
                'k2': post_process_function('post_process_me'),
                'k3': {
                    'kd1': True,
                    'kd2': [
                        {
                            'kdl1': 'u_need_me',
                            'kdl2': 42
                        },
                        {
                            'kdl1': 'u_need_me_too',
                            'kdl2': 42
                        }
                    ]
                }
            }
    """

    # when encoding a mapping of the list
    # the first value in that list should be the corresponding key name
    #   in the input dictionary and
    # the second value should be the dictionary structure itself
    LIST_NAME = 0
    LIST_DICT = 1

    # initiate the output dictionary
    out_dict = {}
    for k, v in map.items():
        if isinstance(v, MDField):
            # start the journey to the path from the first level
            out_dict[k] = in_dict.get(v.path[0], None)
            # keep going down the levels until reached the value
            for level in v.path[1:]:
                # return if the value is missing
                if not out_dict[k]:
                    break
                # reassign with the value at the next level
                out_dict[k] = out_dict[k][level]
            # if the value is required but absent
            if v.is_required & (out_dict[k] is None):
                raise Exception(f"A value for the field '{k}' is required. "
                                f"Field {v.path} is missing or null")
            # if the post processing function is also there
            if v.post_process:
                # apply it to the retrieved value
                out_dict[k] = v.post_process(out_dict[k])
        elif isinstance(v, dict):
            # call function recursively to map the internal dict
            out_dict[k] = map_dictionary(in_dict, v)
        elif isinstance(v, list):
            # initiate the list
            out_dict[k] = []
            # loop through the values in the corresponding input dictionary list
            for list_dict in in_dict[v[LIST_NAME]]:
                # get the upper fields except for the list field itself
                upper_level_fields = {key: val for key, val in in_dict.items() if key != v[LIST_NAME]}
                # add the upper level fields in case required
                list_dict = {**list_dict, **upper_level_fields}
                # call function for each dictionary and append the results
                out_dict[k].append(map_dictionary(list_dict, v[LIST_DICT]))
        # just assign if the value is a predefined constant
        else:
            out_dict[k] = v
    return out_dict


def si_classifier(val):
    suffixes = {
        24:{'long_suffix':'yotta', 'short_suffix':'Y', 'scalar':10**24},
        21:{'long_suffix':'zetta', 'short_suffix':'Z', 'scalar':10**21},
        18:{'long_suffix':'exa', 'short_suffix':'E', 'scalar':10**18},
        15:{'long_suffix':'peta', 'short_suffix':'P', 'scalar':10**15},
        12:{'long_suffix':'tera', 'short_suffix':'T', 'scalar':10**12},
        9:{'long_suffix':'giga', 'short_suffix':'G', 'scalar':10**9},
        6:{'long_suffix':'mega', 'short_suffix':'M', 'scalar':10**6},
        3:{'long_suffix':'kilo', 'short_suffix':'k', 'scalar':10**3},
        0:{'long_suffix':'', 'short_suffix':'', 'scalar':10**0},
        -3:{'long_suffix':'milli', 'short_suffix':'m', 'scalar':10**-3},
        -6:{'long_suffix':'micro', 'short_suffix':'µ', 'scalar':10**-6},
        -9:{'long_suffix':'nano', 'short_suffix':'n', 'scalar':10**-9},
        -12:{'long_suffix':'pico', 'short_suffix':'p', 'scalar':10**-12},
        -15:{'long_suffix':'femto', 'short_suffix':'f', 'scalar':10**-15},
        -18:{'long_suffix':'atto', 'short_suffix':'a', 'scalar':10**-18},
        -21:{'long_suffix':'zepto', 'short_suffix':'z', 'scalar':10**-21},
        -24:{'long_suffix':'yocto', 'short_suffix':'y', 'scalar':10**-24}
    }
    exponent = int(math.floor(math.log10(abs(val))/3.0)*3)
    return suffixes.get(exponent, None)


def si_formatter(value):
    '''
    Return a triple of scaled value, short suffix, long suffix, or None if
    the value cannot be classified.
    '''
    classifier = si_classifier(value)
    if classifier == None:
        # Don't know how to classify this value
        return None

    scaled = value / classifier['scalar']
    return (scaled, classifier['short_suffix'], classifier['long_suffix'])


def si_format(value, precision=0, long_form=False, separator=''):
    '''
    "SI prefix" formatted string: return a string with the given precision
    and an appropriate order-of-3-magnitudes suffix, e.g.:
        si_format(1001.0) => '1.00K'
        si_format(0.00000000123, long_form=True, separator=' ') => '1.230 nano'
    '''

    if value == 0:
        # Don't know how to format this value
        return '0'

    scaled, short_suffix, long_suffix = si_formatter(value)

    if scaled == None:
        # Don't know how to format this value
        return value

    suffix = long_suffix if long_form else short_suffix

    return '{scaled:.{precision}f}{separator}{suffix}'.format(
        scaled=scaled, precision=precision, separator=separator, suffix=suffix)