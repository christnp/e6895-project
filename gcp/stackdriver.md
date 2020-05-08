# GCP Stackdrive Logging Journey
This is the trials and tribulations of setting up and using logging. As with
most things, there are many ways to accomplish the same goal. These are the steps
I took to achieve the final goal (as I remember them...). This may not be the most
effecient way, but it worked. (I have re-oredered the tasks in the most logical
progression, since I arrived at the solution via "trial-and-error")

## Initial config
First, open the Logging UI associated with your project on the Google Cloud 
Platform (GCP). Next, select the "CREATE SINK" button (at the time of this
document, this button was near the top-center of the UI). Create a "Sink Name" 
(this is the name of the sink you will reference in your Python/Scala application).
Select the "Sink Service" (i.e., where you will store the logs) -- I chose
Cloud Storage and selected my project bitbucket. Sidebar: 
> This is not really a quote, I just want to emphasize this statement. Choosing
> the "Sink Service" is an important step in the process and is actually the
> main reason I chose to use the GCP Logging UI. It is important because this step
> setups/configures access for your logging sink. After reading several 
> blogs/forums about adding permisions I releazed this was as easy as setting up
> the sink using the Logging UI vs. Python/Scala API. (FWIW, I know I saw a blip
> on how to configure this via the API in one of the several blogs I read, but
> I couldn't seem to find the blog again!)
Okay, back to the process. Really, that's it for setting up the sink (of course, 
select the correct location, e.g. I chose my top-level bit-bucket). 

Okay, that didn't work as well as I thought... I was still getting a permission error:
```
```

So after more looking around, I added the following when creating my cluster:
``` bash
...
--properties="dataproc:dataproc.logging.stackdriver.enable=true,dataproc:dataproc.monitoring.stackdriver.enable=true" \
--scopes=cloud-platform,monitoring,logging-write \
...
```

And these properties will extract the logs from the master and each of the worker
nodes:
```bash
--properties="yarn:yarn.log-aggregation-enable=true,yarn:yarn.nodemanager.remote-app-log-dir=gs://${BUCKET}/logs,yarn:yarn.log-aggregation.retain-seconds=-1" \
```
Ref-1, Properties: https://cloud.google.com/dataproc/docs/guides/logging#enabling_job_driver_logs_in  
Ref-2, Scopes: https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create#--scopes
Ref-3, Service Account: https://cloud.google.com/logging/docs/api/tasks/exporting-logs#writing_to_the_destination (didn't actually use this?)


I added this to submitting the job (for profiling):
``` bash
...
--properties='cloud.profiler.enable=true,cloud.profiler.name=profiler_name,cloud profiler.service.version=version' \  
...
```
Ref-1, Properties: https://cloud.google.com/dataproc/docs/guides/profiling#pyspark_example  