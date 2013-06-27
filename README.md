Search results clustering for ElasticSearch
===========================================

This clustering plugin adds on-the-fly text clustering capability
to an ElasticSearch node. Clustering algorithms from the Carrot2
project (open source) or proprietary Lingo3G algorithm from
Carrot Search can be used for actual clustering implementation.

Installation
------------

In order to install the plugin, run: 
    
    mvn clean package
    
and copy the file from `target/releases/*.zip` into ElasticSearch's
`plugins/elasticsearch-carrot2` folder. This is the only way of
installing the plugin until a proper release is made to Maven Central.

Usage guide
-----------

Once installed, restart ElasticSearch and point your browser to:
http://localhost:9200/_plugin/elasticsearch-carrot2/index.html
(or wherever your ES node is deployed). That file contains
some sample data and query examples.


Versions and compatibility
--------------------------

    ----------------------------------------------
    | Thrift Plugin | Elasticsearch    | Carrot2 |
    ----------------------------------------------
    | master        | 0.90   -> master | 3.7.1   |
    ----------------------------------------------


License
-------

This software is licensed under the Apache 2 license. Full text
of the license is in the repository (`elasticsearch-carrot2.LICENSE`).
