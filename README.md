Search results clustering for ElasticSearch
===========================================

This clustering plugin adds on-the-fly text clustering capability
to an ElasticSearch node. Clustering algorithms from the Carrot2
project (open source) or proprietary Lingo3G algorithm from
Carrot Search can be used for actual clustering implementation.


Installation
------------

In order to install a stable version of the plugin, 
run ElasticSearch's `plugin` utility (remember to pick the
compatible version of the plugin from the table below).

    bin/plugin --install org.carrot2/elasticsearch-carrot2/1.2.2

To install from sources (master branch), run:

    mvn clean package
    
and unzip `target/releases/*.zip` into ES's plugins subfolder of
your choice.


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

Recommended compatiblity chart (matching versions of ES, Carrot2, 
and optionally Lingo3G).

    ------------------------------------------------------------------
    | Clustering Plugin | Elasticsearch          | Carrot2 | Lingo3G |
    ------------------------------------------------------------------
    | 1.2.2             | 0.90.10-> 0.90.13      | 3.8.0   |  1.8.0  |
    | 1.2.1             | 0.90.10-> 0.90.11      | 3.8.0   |  1.8.0  |
    | 1.2.0             | 0.90.4 -> 0.90.9       | 3.8.0   |  1.8.0  |
    | 1.1.1             | 0.90.4 -> 0.90.9       | 3.8.0   |  1.8.0  |
    | 1.1.0             | 0.90.2 -> 0.90.3       | 3.8.0   |  1.8.0  |
    | 1.0.1             | 0.90   -> 0.90.3       | 3.7.1   |  1.7.1  |
    | 1.0.0             | 0.90   -> 0.90.3       | 3.7.1   |  1.7.1  |
    ------------------------------------------------------------------


License
-------

This software is licensed under the Apache 2 license. Full text
of the license is in the repository (`LICENSE.txt`).
