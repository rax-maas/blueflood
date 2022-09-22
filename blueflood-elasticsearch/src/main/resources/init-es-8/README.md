# Elasticsearch init

These are the init files for Elasticsearch 8. This version removes mapping types, so some code changes will be necessary
to adapt to it.

These are just an initial stab at the configs. Since Blueflood won't work with version 8 at the time of this writing,
testing has been minimal. Let this serve as a starting point, and adapt as needed.

## Changes for version 8

- Most notably, mapping types are removed. There's one hardcoded "type" named `_doc` that's used in api paths that used
  to have a "type" path paremeter in them. See
  https://www.elastic.co/guide/en/elasticsearch/reference/7.17/removal-of-types.html

## Future notes

As originally recorded in the root `init-es.sh` script, here are some notes that will affect future Elasticsearch
upgrades.

- Support for tagged metrics is a possible future feature. The best way to map tags in Elasticsearch is probably with a
  [nested field type](https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html) to avoid [mapping
  explosion](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html#mapping-limit-settings):

  ```json
  "tags": {
    "type": "nested",
    "properties": {
      "key": {
        "type": "keyword"
      },
      "value": {
        "type": "keyword"
      }
    }
  }
```
