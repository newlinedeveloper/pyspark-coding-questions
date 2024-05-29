### Processing 25GB of data in spark | How many executors and how much memory per executor is required

To Process 25 GB data in spark
- How many CPU cores are required ?
- How many executors are required ?
- How much each executor memory is required ?
- what is the total memory required ?

**Notes**

```
By default, spark creates one partition for each block -  128MB - Partition size
we need 2 - 5 maximum core for each executors - high performance
expected memory for each core = minimum 4 * (default partition size) = 4 * 128 MB = 512 MB
Executor memory is not less than 1.5 times of spark reserved memory (single core executor memory should not less than 450 MB )
```


**How many CPU cores are required ?**
25 GB = 25 * 1024 MB = 25600 MB
No of partition = 25600 MB / 128 MB = 200
no of cpu cores = no of partitions = 200

**How many executors are required ?**
Avg CPU cores for each executor = 4
Total no of executor = 200 / 4 =  50


**How much of each executor's memory is required ?**
CPU cores for each executor = 4
Memory for each executor = 4 * 512 MB = 2 GB

**what is the total memory required ?**
Total no of executor =  50
Memory for each executor = 2GB
Total memory for all the executor = 50*2 GB =  100 GB
