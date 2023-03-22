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


# 🎯 How should I use this package effectively? 🎯

- To begin, just simply import the module, run `diagnose_cluster` then `display_summary`end of your notebook after your pipeline has finished running
- Then create a job and run this job on a `job only cluster` (Meaning choose a new job cluster when creating the databricks job) 
- Thats all! You can refer to an example below in the *Usage* section

# How to run on a job only cluster? (Or how to schedule a job in databricks)
- Learn more about databricks jobs and what it can do [here](https://docs.databricks.com/workflows/jobs/jobs.html)

# 🔎 Now I know the problems but how do i solve them? 🔎
- WIP

# Usage 

to get started, clone or import the project




# Additional notes
** You can run this on interactive clusters as well (i.e. Team based cluster, like analytics-transport cluster). However, if you import and use in interactive notebooks / clusters, note that other users and notebooks may be using the same team-based cluster, hence you can see all the issues detected for all queries ran on that cluster

%md

# Roadmap
- Provide a package to "Auto Treat" based on the task size
- Provide visibility on query plans and bottlenecks in query plans leveraging on Graph Theory