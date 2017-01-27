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

    bin/plugin install org.carrot2/elasticsearch-carrot2/5.1.1

To install from sources (master branch), run (if you have Gradle
installed alreadty):

    gradle clean build

or use the provided bootstrap script:

    gradlew clean build

then install with:

    Linux:
    bin/plugin install file:/.../(plugin)/build/distributions/*.zip

    Windows:
    bin/plugin install file:///c:/.../(plugin)/build/distributions/*.zip

Starting with ES 2.2.0, the installer will request confirmation 
concerning extended security permissions. You have to accept it.

Usage
-----

To play with the examples in the documentation, you'll have to allow 
CORS requests from null (if opened directly) or localhost (if served 
by some local HTTP server). Add the following to ES/config/elasticsearch.yml:

```
# Allow localhost cross-origin requests.
http.cors.enabled: true
http.cors.allow-origin: /(null)|(https?:\/\/localhost(:[0-9]+)?)/
```

More information about security implications of enabling CORS are here:
https://www.elastic.co/guide/en/elasticsearch/reference/5.0/modules-http.html

Finally, start ES and open up the documentation in your browser 
(can be opened as a file resource):
  
(plugin sources)/doc/index.html

Alternatively, you can allow CORS headers from cdn.rawgit.com and open 
the documentation directly from there:

https://cdn.rawgit.com/carrot2/elasticsearch-carrot2/master/doc/index.html

CURL request examples are available here:

https://github.com/carrot2/elasticsearch-carrot2/tree/master/doc/curl/


Versions and compatibility
--------------------------

Recommended compatibility chart (matching versions of ES, Carrot2, 
and optionally Lingo3G). (+) means it'll probably work with newer
releases (we test against latest version from that branch). 

Starting with ES 2.0, the plugin is compiled against *an exact* version of ES
and *will not work* with any other version. The numbering of the plugin
will always correspond to the numbering of ES to easily identify
the version of ES the plugin will work with. The only exceptions from this rule
will be critical bugfixes, which will have the fourth version number: then
the first three numbers denote ES release the plugin is compiled against.

If you need a point version that has not been released (yet or skipped),
then update the project descriptor (pom.xml) and recompile from sources,
this will yield a binary version of the plugin compatible with the 
given ES version.

    ------------------------------------------------------------------
    | Clustering Plugin, ES (matching versions)  | Carrot2 | Lingo3G |
    ------------------------------------------------------------------
    | (master, unreleased)                       | 3.15.0  | 1.15.0  |
    | 5.1.1                                      | 3.15.0  | 1.15.0  |
    | 2.4.2 -> 2.4.3                             | 3.15.0  | 1.15.0  |
    | 2.4.1.1                                    | 3.15.0  | 1.15.0  |
    | 2.4.1 -> 2.4.1                             | 3.14.0  | 1.14.0  |
    | 2.4.0 -> 2.4.0.1                           | 3.12.0  | 1.13.0  |
    ------------------------------------------------------------------

Discontinued version branches:

    ------------------------------------------------------------------
    | Clustering Plugin | Elasticsearch          | Carrot2 | Lingo3G |
    ------------------------------------------------------------------
    | 2.3.0 -> 2.3.4                             | 3.12.0  | 1.13.0  |
    | 2.2.1                                      | 3.12.0  | 1.13.0  |
    | 2.2.0                                      | 3.11.0  | 1.12.3  |
    | 2.1.0 -> 2.1.2                             | 3.11.0  | 1.12.3  |
    | 2.0.0 -> 2.0.2                             | 3.11.0  | 1.12.3  |
    | 1.9.1             | 1.6.0  -> 1.7.2+?      | 3.10.4  | 1.12.3  |
    | 1.9.0             | 1.6.0  -> 1.7.0+?      | 3.10.1  | 1.12.0  |
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
