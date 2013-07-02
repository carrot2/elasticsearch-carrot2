Search results clustering for ElasticSearch
===========================================

This clustering plugin adds on-the-fly text clustering capability
to an ElasticSearch node. Clustering algorithms from the Carrot2
project (open source) or proprietary Lingo3G algorithm from
Carrot Search can be used for actual clustering implementation.


Installation
------------

In order to install the plugin, run ElasticSearch's `plugin` utility: 

    bin\plugin --install org.carrot2/elasticsearch-carrot2/1.0.0


Usage guide
-----------

Once installed, restart ElasticSearch and point your browser to:
<http://localhost:9200/_plugin/carrot2/>
(or wherever your ES node is deployed). That file contains
some sample data and query examples.

Alternatively, the plugin's folder contains `_site/curl/` sub-folder
which in turn contains sample `curl` scripts that inject data into
ES and query the clustering plugin.


Versions and compatibility
--------------------------

    --------------------------------------------------
    | Clustering Plugin | Elasticsearch    | Carrot2 |
    --------------------------------------------------
    | master            | 0.90   -> master | 3.7.1   |
    | 1.0.0             | 0.90   -> master | 3.7.1   |
    --------------------------------------------------


License
-------

This software is licensed under the Apache 2 license. Full text
of the license is in the repository (`LICENSE.txt`).
