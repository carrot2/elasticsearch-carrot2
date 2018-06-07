#!/bin/bash

# pre-open-source
# bin/x-pack/setup-passwords interactive

# post-open-source
# bin\elasticsearch-setup-passwords auto

# setup 'elastic' pwd to 'abcdef' (or modify below).

curl -XPOST -u elastic:abcdef -H "Content-Type: application/json" 'http://localhost:9200/_xpack/security/role/clustering' -d '
{
  "cluster": ["monitor"],
  "indices": [
    {
      "names": [ "*" ],
      "privileges": ["read", "index", "view_index_metadata"]
    }
  ]
}'

curl -XPOST -u elastic:abcdef -H "Content-Type: application/json" 'http://localhost:9200/_xpack/security/user/clustering' -d '
{
  "password" : "abcdef",
  "roles" : [ "clustering" ],
  "full_name" : "Clustering User (minimum)"
}'
