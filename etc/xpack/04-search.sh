#!/bin/bash

# Index some sample data. We don't store the fields although in real life it'd make
# more sense to do so (so that we don't have to reparse for clustering).
. creds.sh

curl -XPOST $OPTS -H "Content-Type: application/json" 'http://localhost:9200/test/test/_search?pretty=true&error_trace=true' -d '
{
        "_source" : [
          "url", 
          "title", 
          "content"
        ],
        "query" : {
            "match" : {
              "content" : "data mining" 
            }
        },
        "size": 2
}'
