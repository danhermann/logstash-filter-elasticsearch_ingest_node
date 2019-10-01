package co.elastic.logstash.plugins.filters;

import co.elastic.logstash.api.Event;
import org.elasticsearch.ingest.IngestDocument;
import org.logstash.Javafier;
import org.logstash.Valuefier;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

class IngestMarshaller {

    static final String INGEST_TIMESTAMP = "timestamp";

    private IngestMarshaller() {
    }

    static void toEvent(IngestDocument document, Event e) {
        Map<String, Object> source = document.getSourceAndMetadata();
        Map<String, Object> metadata = document.getIngestMetadata();

        // handle timestamp separately
        ZonedDateTime z = (ZonedDateTime)metadata.get(INGEST_TIMESTAMP);
        if (z != null) {
            e.setEventTimestamp(z.toInstant());
        }

        // handle version separately
        String version = (String)source.get(org.logstash.Event.VERSION);
        if (version != null) {
            e.getData().put(org.logstash.Event.VERSION, version);
        }

        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if (!entry.getKey().equals(org.logstash.Event.VERSION)) {
                e.setField(entry.getKey(), Valuefier.convert(entry.getValue()));
            }
        }

        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            if (!entry.getKey().equals(INGEST_TIMESTAMP)) {
                e.setField(entry.getKey(), Valuefier.convert(entry.getValue()));
            }
        }
    }

    static IngestDocument toDocument(Event e) {
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> metadata = new HashMap<>();

        // handle timestamp separately
        metadata.put(INGEST_TIMESTAMP, e.getEventTimestamp().atZone(ZoneOffset.UTC));

        for (Map.Entry<String, Object> entry : e.getData().entrySet()) {
            if (!entry.getKey().equals(org.logstash.Event.TIMESTAMP)) {
                data.put(entry.getKey(), Javafier.deep(entry.getValue()));
            }
        }

        for (Map.Entry<String, Object> entry : e.getMetadata().entrySet()) {
            metadata.put(entry.getKey(), Javafier.deep(entry.getValue()));
        }

        return new IngestDocument(data, metadata);
    }
}
