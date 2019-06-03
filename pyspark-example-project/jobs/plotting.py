"""
plotting.py
~~~~~~~~~~

This script plots TODAY_LOAD, PREV_DAY_LOAD, ADJ_LOAD (3 series) onto Y-axis and 2/14 (Hourly) onto X-Axis.
This script can be executed as follows,

$SPARK_HOME/bin/spark-submit \
--master spark://localhost:7077 \ #change it to the spark master cluster url
--py-files packages.zip \
--files configs/etl_config.json \
jobs/plotting.py
"""

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib.pyplot as plt

def main():
    """Main ETL script definition.

    :return: None
    """
    #Read in the output.csv from the previous ETL job
    output_pd = pd.read_csv('tests/test_data/energy/data/output.csv')
    
    #Add a datetime column for the plotting purpose
    count = len(output_pd.index)
    datetime = pd.date_range('2/14/2019',periods = count, freq = '1h')
    output_pd.insert(0,"DATETIME",datetime)
    
    #Plotting the first chart for Today_Load
    plt.rcParams['font.size'] = 10
    plt.plot(output_pd.DATETIME,output_pd.TODAY_LOAD)
    plt.title("Today_Load Hourly")
    plt.xlabel("Date Hour (H)")
    plt.ylabel("Today_Load (Watt)")
    plt.ylim(0,110000)
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.gcf().autofmt_xdate()
    plt.show()

    #Plotting the second chart for PREV_Day_Load
    plt.plot(output_pd.DATETIME,output_pd.PREV_DAY_LOAD)
    plt.title("PREV_Day_Load Hourly")
    plt.xlabel("Date Hour (H)")
    plt.ylabel("PREV_Day_Load (Watt)")
    plt.ylim(0,110000)
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.gcf().autofmt_xdate()
    plt.show()

    #Plotting the third chart for ADJ_Load_Hourly
    plt.plot(output_pd.DATETIME,output_pd.ADJ_LOAD)
    plt.title("ADJ_Load Hourly")
    plt.xlabel("Date Hour (H)")
    plt.ylabel("ADJ_Load (Watt)")
    plt.ylim(0,110000)
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.gcf().autofmt_xdate()
    plt.show()


# entry point for Python plotting application
if __name__ == '__main__':
    main()
