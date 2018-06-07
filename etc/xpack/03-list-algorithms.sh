#!/bin/bash

# Index some sample data. We don't store the fields although in real life it'd make
# more sense to do so (so that we don't have to reparse for clustering).
. creds.sh

curl -XGET $OPTS -H "Content-Type: application/json" 'http://localhost:9200/_algorithms?pretty=true&error_trace=true' 
