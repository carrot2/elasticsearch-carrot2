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
ES-compatible version of the plugin from the table below!).

    bin/plugin install org.carrot2/elasticsearch-carrot2/2.1.1

To install from sources (master branch), run:

    mvn clean package
    
and unzip `target/releases/*.zip` into ES's plugins subfolder of
your choice.


Usage guide
-----------

Once installed, restart ElasticSearch and point your browser to:
<http://localhost:9200/_plugin/elasticsearch-carrot2/>
(or wherever your ES node is deployed). That file contains
some sample data and query examples.

Alternatively, the plugin's folder contains `_site/curl/` sub-folder
which in turn contains sample `curl` scripts that inject data into
ES and query the clustering plugin.


Versions and compatibility
--------------------------

Recommended compatibility chart (matching versions of ES, Carrot2, 
and optionally Lingo3G). (+) means it'll probably work with newer
releases (we test against latest version from that branch). Starting
with ES 2.0, the plugin is compiled against an exact version of ES
and will not work with any other version. The numbering of the plugin
will always correspond to the numbering of ES to easily identify
the version of ES the plugin will work with.

    ------------------------------------------------------------------
    | Clustering Plugin for ES 2.x+ (matching)   | Carrot2 | Lingo3G |
    ------------------------------------------------------------------
    | 2.1.1                                      | 3.11.0  | 1.12.3  |
    | 2.1.0                                      | 3.11.0  | 1.12.3  |
    | 2.0.2                                      | 3.11.0  | 1.12.3  |
    | 2.0.1                                      | 3.11.0  | 1.12.3  |
    | 2.0.0                                      | 3.11.0  | 1.12.3  |
    | 2.0.0-rc1                                  | 3.11.0  | 1.12.3  |
    ------------------------------------------------------------------

    ------------------------------------------------------------------
    | Clustering Plugin | Elasticsearch          | Carrot2 | Lingo3G |
    ------------------------------------------------------------------
    | 1.9.1             | 1.6.0  -> 1.7.2+?      | 3.10.4  | 1.12.3  |
    | 1.9.0             | 1.6.0  -> 1.7.0+?      | 3.10.1  | 1.12.0  |
    ------------------------------------------------------------------


Discontinued version branches:

    ------------------------------------------------------------------
    | Clustering Plugin | Elasticsearch          | Carrot2 | Lingo3G |
    ------------------------------------------------------------------
    | 1.8.0             | 1.4.0  -> 1.6.0+       | 3.9.3   | 1.10.0  |
    | 1.7.0             | 1.3.0  -> 1.3.5+       | 3.9.3   | 1.10.0  |
    | 1.6.0             | 1.2.0  -> 1.2.2+       | 3.9.2   |  1.9.1  |
    | 1.5.0             | 1.1.0  -> 1.1.2+       | 3.9.2   |  1.9.1  |
    | 1.4.0             | 1.0.0  -> 1.0.3        | 3.9.0   |  1.9.0  |
    | 1.3.1             | 1.0.0  -> 1.0.3        | 3.8.1   |  1.8.1  |
    | 1.3.0             | 1.0.0  -> 1.0.3        | 3.8.1   |  1.8.1  |
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
