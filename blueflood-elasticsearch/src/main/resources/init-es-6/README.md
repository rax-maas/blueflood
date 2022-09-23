# Elasticsearch init

This is a copy of the original init script and files from the root "resources" directory that's tweaked to work for
Elasticsearch 6. Expect to find new, slightly improved copies of all these for each new Elasticsearch that needs
changes.

Elasticsearch 6 is the last major version that supports mapping types without any extra work. Blueflood should be able
to move from Elasticsearch 1.7 to Elasticsearch 6 without code changes, as long as `ENABLE_TOKEN_SEARCH_IMPROVEMENTS` is
turned on. See below for details.

## Changes for version 6

- Most notably, the ["string" type has been removed](https://www.elastic.co/blog/strings-are-dead-long-live-strings), so
  the mappings now use "keyword" instead.

- The custom analyzer from the original `index_settings.json` was used to implement token-based auto-complete with a
  search containing aggregations. That feature has since been handled more efficiently in code, in and around
  `ElasticTokensIO.java` and the `ENABLE_TOKEN_SEARCH_IMPROVEMENTS` setting. Furthermore, in version 6, an analyzer can
  only be set for a `text` field, and there are warnings about running aggregations such as those used here because you
  have to enable `fielddata`, which can consume a lot of resources. See
  https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html#fielddata-mapping-param.

  **IMPORTANT NOTE:** This means the `/v2.0/{tenantId}/metric_name/search` path will always return empty results *unless*
  you have `ENABLE_TOKEN_SEARCH_IMPROVEMENTS` turned on!

  Our plan for moving forward should be to deprecate the code that uses the custom analyzer, and plan to remove it to
  clear the way for easier integration testing of Elasticsearch 6+. Otherwise, we have to juggle tests for the code that
  only works in version 1.x and those that work for all versions.

## Future notes

As originally recorded in the root `init-es.sh` script, here are some notes that will affect future Elasticsearch
upgrades.

- Elasticsearch 7 deprecates mapping types. They'll still work in 7 but require you to pass a query parameter to some
  API calls. See https://www.elastic.co/guide/en/elasticsearch/reference/7.17/removal-of-types.html

- Elasticsearch 8 removes mapping types entirely. There's one hardcoded "type" named `_doc` that's used in all api paths
  that used to have a "type" path parameter in them.

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
