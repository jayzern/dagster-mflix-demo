from dagster import MonthlyPartitionsDefinition


START_DATE = '2011-01-01'
END_DATE = '2016-01-01'

monthly_partition = MonthlyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE
)
