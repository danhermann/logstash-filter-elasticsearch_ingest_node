package co.elastic.logstash.plugins.filters;

import org.junit.Test;

public class IngestMarshallerTest {

    @Test
    public void testToDocument() {
        IngestMarshaller.toDocument(null);
    }

    @Test
    public void testToEvent() {
        IngestMarshaller.toEvent(null, null);
    }
}
