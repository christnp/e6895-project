# README

The data used for this script is currently located in the `{project_top}/data/IPIP-NEO/input`
directory. It is also located on the `gs://eecs-e689-bucket/input` location for
the distributed Spark jobs. 

To run the code, simply run the `create _cluster` shell script followed by the
`submit_job` shell script. This will create a cluster with an `n1-highmem-8`
master node and two `n1-highmem-8` worker nodes. This, along with more optimized
partitioning of the dataframe, was needed to compensate for the out of memory
errors encoutered with the first attempt. 