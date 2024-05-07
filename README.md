# test_task
For this task, I've created a function that automatically downloads all Parquet files from the year 2024 and beyond. Additionally, another function appends all monthly files of each year and generates a combined file, resembling a process of data integration.
Furthermore, I utilized the combined file to compute the average length of trips for each month and implemented a rolling average calculation.
To streamline the process, I've built a Docker image and set up a CI/CD pipeline. Below are the instructions to run the code:

1. Clone the Repository: git clone https://github.com/subashpdl/test_task.git
2. Install Requirements: pip install -r requirements.txt
3. Run the Code: python main.py
There are differnt approaches to tackling this task. For production-level solutions, deploying a Docker container would be great option. Additionally, leveraging Apache Airflow for task orchestration, such as scheduling scripts to run monthly, can enhance efficiency and maintainability.

In my opinion, The best practice is creating fixed schema for all files, followed by the ingestion of data into this schema, especially on platforms like HDFS or various cloud services, to ensure efficient management of large datasets and then calcuating or analyzing the data.

# Scalling Data Pipeline: 
For this task, considering the extensive timeframe of yellow taxi trip record data spanning from 2009 until February 2024, integrating all the data might be time-consuming. Hence, the most viable approach would be distributed data processing, facilitated by systems like the Python library Dask. Dask enables parallel and distributed computing, ensuring scalability for data processing and machine learning tasks. Alternatively, Spark serves as another robust option for scalable data processing, offering distributed processing capabilities and fault tolerance, making it suitable for handling large-scale datasets across clusters of machines. However, it's imperative to initially determine the approximate size of the data. If the data is relational, storing it in SQL databases would be advantageous, as it allows processing through SQL queries or other systems like Spark or Python, leveraging the powerful ACID (Atomicity, Consistency, Isolation, Durability) features of SQL databases.
If scaling up my pipeline becomes necessary, my approach would be to store the data in HDFS (Hadoop Distributed File System) or AWS S3, or Azure Datalake, or lakehouse and leverage PySpark for processing. This setup offers several advantages for scaling operations. HDFS or other storage provide a distributed storage solution that can accommodate large volumes of data across multiple nodes, ensuring fault tolerance and high availability. Meanwhile, PySpark allows for distributed data processing, enabling parallel execution of tasks across the cluster. By combining HDFS or others storage and PySpark, I can efficiently manage and process large-scale datasets while benefiting from the scalability and performance.



