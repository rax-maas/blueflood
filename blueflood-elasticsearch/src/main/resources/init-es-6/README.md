# Elasticsearch init

This is a copy of the original init script and files from the root "resources" directory that's tweaked to work for
Elasticsearch 6. Expect to find new, slightly improved copies of all these for each new Elasticsearch that needs
changes.

Elasticsearch 6 is the last major version that supports mapping types without any extra work. Blueflood should be able
to move from Elasticsearch 1.7 to Elasticsearch 6 without code changes.

## Changes for version 6

- Most notably, the ["string" type has been removed](https://www.elastic.co/blog/strings-are-dead-long-live-strings), so
  the mappings now use "keyword" instead.

- The apparently experimental custom analyzer from the original `index_settings.json` seems like it was trying to implement
  token-based auto-complete. That feature has since been handled in code, in and around `ElasticTokensIO.java`. Therefore,
  I've removed the custom index settings.

## Future notes

As originally recorded in the root `init-es.sh` script, here are some notes that will affect future Elasticsearch
upgrades.

- Elasticsearch 7 deprecates mapping types. They'll still work in 7 but require you to pass a query parameter to some
  API calls. See https://www.elastic.co/guide/en/elasticsearch/reference/7.17/removal-of-types.html

- Elasticsearch 8 removes mapping types entirely. There's one hardcoded "type" named `_doc` that's used in all api paths
  that used to have a "type" path parameter in them.
