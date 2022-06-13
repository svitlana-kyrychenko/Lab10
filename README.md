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

second query

![image](https://user-images.githubusercontent.com/102665740/173281688-2f54e2c1-d193-4ed8-abb3-84b05cc22872.png)

third query

![image](https://user-images.githubusercontent.com/102665740/173283201-ef6bab96-548c-4fd3-aa9e-4a19a170b9d1.png)

fourth query

![image](https://user-images.githubusercontent.com/102665740/173283287-da90b553-c438-48ad-af49-4102029c5615.png)

fifth query

![image](https://user-images.githubusercontent.com/102665740/173283359-88472d34-e304-402f-8ebb-1ecd05e266be.png)

sixth query

![image](https://user-images.githubusercontent.com/102665740/173283903-1fa81525-0a94-4938-8237-708e5297071b.png)
