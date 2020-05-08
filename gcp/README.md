# Some notes regarding GCP

## Submitting a Spark Scala job on existing Dataproc cluster
1. Start a cluster: (enable Dataproc API)
```
gcloud beta dataproc clusters create wordcount-test \
--image-version=1.4-ubuntu18 --enable-component-gateway \
--bucket eecs-e6895-bucket --project eecs-e6895-edu --single-node 
```

2. Submit the job:
``` 
gcloud dataproc jobs submit spark \
    --cluster=wordcount-test \
    --class=WordCount \
    --jars=gs://eecs-e6895-bucket/jars/sparkwordcount-0.0.2.jar \
    -- gs://eecs-e6895-bucket/input/othello.txt gs://eecs-e6895-bucket/output/result.txt \
    --region=us-west1 
```

Scripts are located in /scripts
