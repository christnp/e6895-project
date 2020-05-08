# Instructions

## Setting up neo4j for Apache Spark
This section describes the steps used to setup the neo4j ecosystem

### Locally on Linux (tbd)
0. Install/configure Apache Spark (for scala)
    * https://intellipaat.com/blog/tutorial/spark-tutorial/downloading-spark-and-getting-started/
    * Apache Spark 2.4.5 with Hadoop Pre-built 2.7 (comes with Scala 2.11.12)
        * https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
    * Scala 2.11.12 (GCP image 1.4-ubuntu18 uses Spark 2.4.5 w/ Scala 2.11.12)
        * https://www.scala-lang.org/download/2.11.12.html
    * Apache Maven (for Compiling Scala): https://docs.cloudera.com/documentation/enterprise/5-5-x/topics/spark_building.html#building
        * create directory structure for Maven projects
        * update pom.xml to include dependencies (this is an iterative process usually, with the the following command)
        * run ```mvn clean install```
        * (update) also run ```mvn assembly:assembly -DdescriptorId=jar-with-dependencies``` to include dependencies (specifically, BigQuery)
        * to see dependencies included run ```mvn dependency:tree```
        * use this to copy to GCP bucket: ```gsutil cp {file_to_copy} gs://{bucket_name}/{location_to_save_to}```
    * Examples: https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples
        * Run standalone cluster: https://supergloo.com/spark-scala/apache-spark-cluster-run-standalone/
        * More on standalone: https://spark.apache.org/docs/latest/spark-standalone.html
        * Has some info on setting Spark Conf: https://mbonaci.github.io/mbo-spark/
        * ```spark-submit --class WordCount --master spark://zeus:7077 target/sparkwordcount-0.0.1.jar```

1. Install/configure docker
    * (used Option 1) https://phoenixnap.com/kb/how-to-install-docker-on-ubuntu-18-04
    * Add yourself to the docker group (?)
        * https://www.digitalocean.com/community/questions/how-to-fix-docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket
        * You have to logout and back in
    
2. Use neo4j docker run command (I added this to a script)
    * https://neo4j.com/developer/docker-run-neo4j/
    * {PROJECT_DIR}/scripts/create_neo4j_docker.sh
    * Use ```docker ps -a``` to status of container (running or failed)
    * Created start/stop scripts in ```[course_dir]/scripts```

3. Access the DB
    * ```localhost:7474```

4. Connect Spark to neo4j
    * https://neo4j.com/developer/apache-spark/

### In GCP
https://spark.apache.org/docs/latest/

Followed these instructions: 

1. Create new project and enable Compute Engine API
    * 
2. Set up local GCP project:
    * if configured from env vars (which mine is):   
    ```export CLOUDSDK_CORE_PROJECT=eecs-e6895-edu```  
    Note: edit ```~/.bashrc```!
    * if configured via gcloud:  
    ```gcloud config set project eecs-e6895-edu```
 
3. https://neo4j.com/developer/neo4j-cloud-google-image/
    * https://neo4j.com/google-cloud-resources/
    * https://cloud.google.com/sdk/install
    * list neo4j images:  
    ```gcloud compute images list --project launcher-public | grep neo4j```
    _reference: https://community.neo4j.com/t/neo4j-3-5-1-added-to-google-cloud-platform-cluster-and-single-node-community-and-enterprise/4174/3_

4. https://medium.com/neo4j/running-neo4j-on-google-cloud-6592c1b4e4e5

I created scripts in ```[course_dir]/scripts```

5. Running cluster locally
https://spark.apache.org/docs/latest/spark-standalone.html

Next steps: 
1. Using GraphQL in Cloud Run (docker container):
    * https://medium.com/google-cloud/secure-graphql-apis-in-minutes-with-google-cloud-run-and-grand-stack-97d050dbc744
2. Build a custom docker container with our DB
    * https://neo4j.com/developer/docker-run-neo4j/

3. Self healing graph DB using clusters?
    * https://neo4j.com/docs/operations-manual/current/docker/

### In AWS (tbd)
1. https://neo4j.com/developer/neo4j-cloud-aws-ec2-ami/



# Other stuff
### Page Rank
Look into caching https://spark.apache.org/docs/latest/quick-start.html#caching

Psychology: https://ipip.ori.org/newPublications.htm
