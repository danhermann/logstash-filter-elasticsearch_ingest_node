package co.elastic.logstash.plugins.filters;

import org.elasticsearch.ingest.Pipeline;

public interface PipelineProvider {

    Pipeline getPipelineByName(String name);
}
