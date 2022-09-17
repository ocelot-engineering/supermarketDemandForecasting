#
# Functions used in testing and validation
#
import pyspark


def assert_col_has_no_null(dat: pyspark.sql.dataframe.DataFrame, col_name: str):
    """
    Asserts a column in a Spark data frame has no missing values
    :param dat: Spark dataframe
    :param col_name: name of column in spark dataframe
    :return:None
    """
    assert dat.filter(col_name + " is null").count() == 0, "Nulls found in '" + col_name + "' column"
