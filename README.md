# test_task
For this task, I've created a function that automatically downloads all Parquet files from the year 2024 and beyond. Additionally, another function appends all monthly files of each year and generates a combined file, resembling a process of data integration.
Furthermore, I utilized the combined file to compute the average length of trips for each month and implemented a rolling average calculation.
To streamline the process, I've built a Docker image and set up a CI/CD pipeline. Below are the instructions to run the code:

1. Clone the Repository: git clone https://github.com/subashpdl/test_task.git
2. Install Requirements: pip install -r requirements.txt
3. Run the Code: python main.py
There are differnt approaches to tackling this task. For production-level solutions, deploying a Docker container would be great option. Additionally, leveraging Apache Airflow for task orchestration, such as scheduling scripts to run monthly, can enhance efficiency and maintainability.

In my opinion, The best practice is creating fixed schema for all files, followed by the ingestion of data into this schema, especially on platforms like HDFS or various cloud services, to ensure efficient management of large datasets and then calcuating or analyzing the data.

#Scalling Datapipeline:


