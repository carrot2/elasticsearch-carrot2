#!/bin/bash

# Index some sample data. We don't store the fields although in real life it'd make
# more sense to do so (so that we don't have to reparse for clustering).
. creds.sh

curl -XPOST $OPTS -H "Content-Type: application/json" 'http://localhost:9200/test/test/_search_with_clusters?pretty=true&error_trace=true' -d '
{
    "search_request": {
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
        "size": 100
    },

    "query_hint": "data mining",
    "field_mapping": {
        "title"  : ["_source.title", "_source.content"]
    }
}'
