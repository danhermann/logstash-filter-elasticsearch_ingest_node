package org.logstashplugins;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public class SetSecurityUserProcessor extends AbstractProcessor {

    public static final String TYPE = "set_security_user";

    private SetSecurityUserProcessor(String tag) {
        super(tag);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        // within Logstash, the set_security_user processor should be a no-op
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public SetSecurityUserProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                               Map<String, Object> config) {
            String[] supportedConfigs = {"field", "properties"};
            for (String cfg : supportedConfigs) {
                config.remove(cfg);
            }
            return new SetSecurityUserProcessor(processorTag);
        }
    }

}
