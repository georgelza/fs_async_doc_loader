# FeatureSpace ASyncOut loader

This is the second version. This version loads directly into a MongoDB/DocumentDB from locally staged files.

Version 1 was called Lambda loader that loaded from S3 source via a S3 event onto Apache Kafka topic, to then be sinked into datastore of chocie utilising Apache Kafka Connect framework.
- [Golang Version](https://github.com/georgelza/GoLambdaLoader)
- [Python Version](https://github.com/georgelza/PythonLambdaLoader)

Current version can be pointed at either a directory container a Hour's files hour=XX or
At a day day=XX or
at entire month month=XX.

## ToDo:

### Targeted Loading
Will add capability to specify a range to load... i.e. 

- either set of hours, 
- or set of days.

### Monitoring

- Prometheus metrics to be pushed to a Prometheus Gateway.

### Logging

- ModifyLog format potentially, to include source file name (and records loaded) in every line => " Month Process Time, St", for improved log analytics.


.