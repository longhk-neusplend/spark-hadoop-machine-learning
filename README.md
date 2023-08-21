# Design machine learning architecture using Apache Spark and Hadoop
[![Hadoop](https://img.shields.io/badge/hadoop-3.2.1-green)](http://apache.github.io/hadoop/)
[![Spark](https://img.shields.io/badge/spark-3.0.2-green)](https://spark.apache.org/docs/latest/api/python/index.html)
[![Posgresql](https://img.shields.io/badge/postgres-11.7-brown)](https://www.postgresql.org/)

This project gives an introduction to setting up machine learning architecture using Apache Spark and Hadoop system. We'll use Apache Spark as the distributed processing system that allows the customer to process their data and train preferred models. [Apache Spark](https://github.com/apache/spark)

## Screenshots & Gifs

**View System**

<div>
    <kbd>
        <img title="View System" src="https://github.com/longhk-neusplend/spark-hadoop-machine-learning/blob/main/public/spark_ml_architecture.png?raw=true" />
    </kbd>
    <br/>
</div>
<br>

## Contents
- [Screenshots & Gifs](#screenshots--gifs)
- [Process](#process)
    - [1. Install docker, docker-compose](https://docs.docker.com/compose/install/)
    - [2. Pull git repo](https://github.com/longhk-neusplend/real-time-analytics.git)
    - [3. Start Server](https://github.com/longhk-neusplend/real-time-analytics#3-start-server)
- [Contact Us](#contact-us)


## Process

### 1. Install docker and docker-compose

`https://www.docker.com/`

### 2. Pull git repo
`git clone https://github.com/longhk-neusplend/spark-hadoop-machine-learning.git` 

### 3. Start Server
`cd spark-hadoop-machine-learning && docker-compose up`

`Apache Spark`

| container       |   Exposed ports  |  User/Password    |
| :-------------: |  :-------------: | :-------------:   |
| spark-master    |     9090,7077    | Local             |
| spark-worker-a  |     9091,7000    | Local             |
| spark-worker-b  |     9092,7001    | Local             |
| demo-database   |     5432         | postgres/casa1234 |

`Apache Hadoop`

| container        |   Exposed ports  |  User/Password   |
| :--------------: |  :-------------: | :-------------:  |
| Namenode         |     9870,9000    | Local            |
| History          |     8188         | Local            |
| Datanode         |     9864         | Local            |
| Nodemanager      |     8042         | Local            |
| Resource manager |     8088         | Local            |


### 3. Create Python file for processing data and training model
 - Machine learning model file at /spark-cluster-v2/apps/ml_model.py declare step by step how to process data, transform it and then use these data for training our model. We use RandomForestClassifier as our main model, this model helps us combine multiple decision trees and makes prediction.
```
   rf = RandomForestClassifier(labelCol=<label column>, featuresCol=<feature column>, maxDepth=<number of depth>)
```

 - Then submit this python file to spark for running and prediction with machine learning model.
<div>
    <img src="./public/druid_connect.gif" />
</div>
<br>

 - After running, we can view the prediction rate and we can validate the result of the prediction.
 - Completed!

## Contact Us
- Business Email: long.hk@neusplend.com - Henry Hoang