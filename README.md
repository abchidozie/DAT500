# DAT-500
Reddit Topic Summarization and Classification
This project is an implementaton of Reddit Classification of comments using Hadoop and Spark.
The project is divided into three major tasks and each task has its own notebooks. The scripts folder also contains scripts
that were used to run map reduce using MrJob.

## Dependencies
One has to install Hadoop locally and set up Spark to work with hadoop
The Java SDK version used to run the project is 1.8
You also need to install MrJob and have Python3.8 on your system
To ensure all clusters have the same environment, please add this configuration while creating your spark session
``.config("spark.yarn.dist.archives","pyspark_venv.tar.gz#environment")\``
