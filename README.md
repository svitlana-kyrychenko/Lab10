# Lab10
Spark data processing

Put file with dataset in folder data

Order of running

> run-cluster.sh
> 
> docker run --rm -it --network spark-network --name spark-submit -v "path":/opt/app bitnami/spark:3 /bin/bash
> 
> cd /opt/app
> 
> spark-submit --master local[*] --deploy-mode client main.py

