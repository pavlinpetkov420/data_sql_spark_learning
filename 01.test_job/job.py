from pyspark.sql import SparkSession, DataFrame

import constants as c
import queries as q

from typing import Union


class Job:

    def __init__(self, file_path: str, delimiter: str=',') -> None:
        self.spark = self.create_spark_session()
        self.__file_path: str = file_path
        self.__delimiter: str = delimiter
        self.df: Union[DataFrame, None] = None

    @property
    def file_path(self) -> str:
        return self.__file_path

    @file_path.setter
    def file_path(self, value) -> None:
        try:
            if isinstance(value, str):
                self.__file_path = value
        except ValueError:
            raise ValueError(c.WRONG_TYPE_ERROR_EXPECT_STRING)

    @property
    def delimiter(self):
        return self.__delimiter

    @delimiter.setter
    def delimiter(self, value):
        try:
            if isinstance(value, str):
                self.__file_path = value
        except ValueError:
            raise ValueError(c.WRONG_TYPE_ERROR_EXPECT_STRING)

    @staticmethod
    def create_spark_session():
        # Prepare sparkSession for the Job
        return SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def open_file(self):
        """
        Open csv file with predefined delimiter & infered schema.
        Please use CSV with header to prevent error during execution.
        :return: DataFrame
        """
        self.df: DataFrame = self.spark.read.\
            options(
                inferSchema='True',
                delimeter=self.__delimiter,
                header=True)\
            .csv(path=self.__file_path)

    def create_view(self, ):
        # Create temp view
        self.df.createOrReplaceTempView(c.financial_year_view_name)

    def filter_agriculture_data(self):
        # Filter only Agriculture data & show
        final_df: DataFrame = self.spark.sql(
            q.financial_year_filter_agriculture
            .format(view_name=c.financial_year_view_name)
        )

        return final_df

    def execution(self):
        self.open_file()
        self.create_view()
        final_df = self.filter_agriculture_data()
        final_df.show()

if __name__ == "__main__":
    print("Job is starting.")
    file_abs_path: str = input("Please enter absolute CSV file path to create DataFrame: ")
    csv_delimiter: str = input("Please enter a delimiter for your csv file: ")

    job = Job(file_path=file_abs_path, delimiter=csv_delimiter)
    job.execution()
