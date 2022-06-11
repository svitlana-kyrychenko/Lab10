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

first query

![image](https://user-images.githubusercontent.com/102665740/173187407-0c0d9c19-f1a7-4f09-a574-423ff76d12cb.png)


