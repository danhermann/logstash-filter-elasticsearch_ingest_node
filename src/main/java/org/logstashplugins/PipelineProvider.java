package org.logstashplugins;

import org.elasticsearch.ingest.Pipeline;

public interface PipelineProvider {

    Pipeline getPipelineByName(String name);
}
