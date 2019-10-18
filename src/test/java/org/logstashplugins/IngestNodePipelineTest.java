package org.logstashplugins;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class IngestNodePipelineTest {

    @Test
    public void testMultiplePipelines() throws IOException {
        List<IngestNodePipeline> pipelines;
        try (final InputStream src = getClass().getResourceAsStream("multiplePipelines.json")) {
            pipelines = IngestNodePipeline.createFrom(src);
        }

        Assert.assertNotNull(pipelines);
        Assert.assertEquals(8, pipelines.size());

        String[] expectedPipelines = {
                "rename_hostname4",
                "rename_hostname5",
                "rename_hostname6",
                "rename_hostname7",
                "rename_hostname8",
                "rename_hostname",
                "rename_hostname2",
                "rename_hostname3",
        };

        for (int k = 0; k < pipelines.size(); k++) {
            Assert.assertEquals(expectedPipelines[k], pipelines.get(k).getName());
        }

    }
}
