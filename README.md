# Introduction to Spark M🐵nkey

Spark monkey is a project that aims to empower non-technical Spark SQL users such as analysts. With Spark Monkey, **metrics and statistics** that are important to **debug a Spark query** are now easily retrievable without digging into the Spark UI. To take it a step further, it can also identify some **crucial and common errors and bottlenecks** in your spark queries.


# 🚨 How can this help me?🚨

These issues are common causes for slow, expensive queries and if users are able to self-diagnose and adopt good practices, general analytics users can efficiently run pipelines that are less time consuming and more efficient


There are 5 common bottlenecks / optimization oppurtunities that are detected by this package. 


- Spill `Detects any partitions with more than 0 bytes of spill`
- Skew `Detects the imbalance in the size of partitions`
- UDFs `Detects prescence of User-Defined Functions`
- Partition / Pushed Filters `Detects absence of partition or pushed filters for tables above 1 million rows`
- Repeated queries `Detects repeated sections of your query - only works on clusters with smaller number of spark jobs (< 20)**` 

# Usage 

To get started, clone or import the project

`pip install git+https://github.com/junronglau/spark-monkey.git`

`from SparkMonkey.spark_monkey import SparkMonkey`

Instantiate the class

`spark_monkey = SparkMonkey(databricks_host_url='adb-12345678.9.azuredatabricks.net')`

After importing, run your queries or pipelines as per normal. At the end of the notebook, perform the analysis on the cluster using

`spark_monkey.diagnose_cluster()`

To display the summary of the issues faced

`spark_monkey.display_summary()`

There are other methods that allow us to retrieve all the Spark jobs or SQL queries in the history server, such as `spark_monkey.retrieve_all_sql()` or `spark_monkey.retrieve_all_jobs()`. Explore the SparkMonkey class for more methods.


# 🔎 How do I solve some of the issues in my pipelines? 🔎
- You can head over to my [Medium article](https://medium.com/@junronglau/speed-up-your-spark-queries-in-15-minutes-4a8da88942cf) to get fixes to common issues


# Experimental features

It is also ideal if this libary can automatically configure settings based on the bottlenecks. One idea is to identify the slow running stages caused by an inefficient shuffle activity, then recommend a shuffle repartitioning to minimize any chance of a data skew/spill. While it is not tested sufficiently, you can try it out by calling the `recommend_shuffle_partition` method

```
recommended_partitions = spark_monkey.recommend_shuffle_partition(stage_id=123)
spark.conf.set("spark.sql.shuffle.partitions", recommended_partitions)
```
