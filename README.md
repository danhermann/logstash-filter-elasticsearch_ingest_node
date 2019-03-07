# Elasticsearch Ingest Node plugin for Logstash (WIP)
## Preliminary documentation

### Description

Execute arbitrary [Elasticsearch ingest node](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest.html)
pipelines on Logstash events. The format for the ingest node pipeline definition is the [same one used by 
Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/pipeline.html).

In the example below, two ingest pipelines would be defined, the `set_and_lower` pipeline that sets the `my_field1` 
field on Logstash events to `FOO BAR BAZ` and then copies the lowercase value of that field to a field named 
`my_field2`. The second pipeline, `rename_hostname`, renames the `hostname` field in events to `host`.

```
{
  "set_and_lower": {
    "processors": [
      {
        "set": {
          "field": "my_field1",
          "value": "FOO BAR BAZ"
        }
      },
      {
        "lowercase": {
          "field": "my_field1",
          "target_field": "my_field2",
          "ignore_missing": false
        }
      },
    ]
  },
  "rename_hostname": {
    "processors": [
      {
        "rename": {
          "field": "hostname",
          "target_field": "host",
          "ignore_missing": true
        }
      }
    ]
  }
}
``` 

All [ingest node processors](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-processors.html) 
are supported with the exception of the `set_security_user` processor. It functions only within the context of an 
authenticated Elasticsearch user and is therefore ignored when run within Logstash.

Possible motivations for running Elasticsearch ingest node pipelines in Logstash include:

* Enabling users to off-load processor-intensive or high-latency enrichment pipelines from Elasticsearch to Logstash.
* If users have existing Elasticsearch ingest pipelines for which they have new requirements that cannot be fulfilled
in Elasticsearch (e.g., external lookup enrichment, multiple outputs), the existing pipeline could be moved to Logstash
and the additional functionality added within the Logstash pipeline.

### Elasticsearch Ingest Node Filter Configuration Options

This plugin supports the following configuration options:

| **Setting**  | **Input type**  | **Required** |
|--------------|-----------------|--------------|
| [node_name](#node_name)    | string          | No           |
| [pipeline_definitions](#pipeline_definitions)  | string  | Yes  |
| [primary_pipeline](#primary_pipeline)  | string  | No  |
| [watchdog_interval](#watchdog_interval)  | string  | No  |
| [watchdog_max_time](#watchdog_max_time)  | string  | No  |


#### node_name

Sets the `node.name` property for all ingest pipelines running within this filter. Defaults to a random UUID.

#### pipeline_definitions

Path to the file containing the JSON definition for the ingest pipelines to be run in this filter.

#### primary_pipeline

The name of the ingest pipeline to run first in the case that multiple pipelines are defined for this filter. Defaults
to first pipeline specified in the pipeline definitions files.

#### watchdog_interval

The interval at which the Grok watchdog will check for long-running Grok operations. Equivalent to the
[`ingest.grok.watchdog.interval`](https://www.elastic.co/guide/en/elasticsearch/reference/current/grok-processor.html#grok-watchdog)
property on the Elasticsearch Grok processor. Defaults to `1s`.

#### watchdog_max_time

The max duration that a Grok operation will be permitted to run before being terminated. Equivalent to the 
[`ingest.grok.watchdog.max_execution_time`](https://www.elastic.co/guide/en/elasticsearch/reference/current/grok-processor.html#grok-watchdog)
property on the Elasticsearch Grok processor. Defaults to `1s`.
