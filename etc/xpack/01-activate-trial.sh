#!/bin/bash

# Post- open source only.
curl -XPOST 'http://localhost:9200/_xpack/license/start_trial?acknowledge=true'