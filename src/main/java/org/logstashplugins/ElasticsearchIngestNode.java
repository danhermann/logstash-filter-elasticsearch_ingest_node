package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.EventFactory;
import co.elastic.logstash.api.Filter;
import co.elastic.logstash.api.FilterMatchListener;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import co.elastic.logstash.api.PluginHelper;
import com.google.common.annotations.VisibleForTesting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.ingest.useragent.IngestUserAgentPlugin;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;

@LogstashPlugin(name = "elasticsearch_ingest_node")
public class ElasticsearchIngestNode implements Filter, PipelineProvider {

    public static final PluginConfigSpec<String> NODE_NAME =
            PluginConfigSpec.stringSetting("node_name");
    public static final PluginConfigSpec<String> WATCHDOG_INTERVAL =
            PluginConfigSpec.stringSetting("watchdog_interval");
    public static final PluginConfigSpec<String> WATCHDOG_MAX_TIME =
            PluginConfigSpec.stringSetting("watchdog_max_time");
    public static final PluginConfigSpec<String> PIPELINE_DEFINITIONS =
            PluginConfigSpec.requiredStringSetting("pipeline_definitions");
    public static final PluginConfigSpec<String> PRIMARY_PIPELINE =
            PluginConfigSpec.stringSetting("primary_pipeline");

    private String id;
    private String nodeName;
    private Map<String, Pipeline> pipelines;
    private Pipeline primaryPipeline;
    private EventFactory eventFactory;

    public ElasticsearchIngestNode(String id, Configuration config, Context context) {
        this(id, config, context, toFileInputStream(config.get(PIPELINE_DEFINITIONS)));
    }

    @VisibleForTesting
    ElasticsearchIngestNode(String id, Configuration config, Context context, InputStream pipelineDefinitions) {
        this.id = id;
        this.eventFactory = context.getEventFactory();
        this.nodeName = config.get(NODE_NAME) == null ? UUID.randomUUID().toString() : config.get(NODE_NAME);
        String primaryPipelineName = config.get(PRIMARY_PIPELINE);
        List<IngestNodePipeline> ingestNodePipelines;
        try {
            ingestNodePipelines = IngestNodePipeline.createFrom(pipelineDefinitions);
        } catch (IOException ex) {
            throw new IllegalStateException("Error creating ingest node filter", ex);
        }

        if (ingestNodePipelines.size() == 0) {
            throw new IllegalStateException("No pipeline definitions found");
        }

        this.pipelines = new Hashtable<>();
        for (IngestNodePipeline p : ingestNodePipelines) {
            pipelines.put(p.getName(), getPipeline(p.getName(), p.toIngestNodeFormat()));
        }

        String resolvedPrimaryPipelineName = primaryPipelineName == null
                ? ingestNodePipelines.get(0).getName()
                : primaryPipelineName;
        primaryPipeline = pipelines.get(resolvedPrimaryPipelineName);
        if (primaryPipeline == null) {
            throw new IllegalStateException(
                    String.format("Could not find primary pipeline '%s'", resolvedPrimaryPipelineName));
        }
    }

    @Override
    public Collection<Event> filter(Collection<Event> incomingEvents, FilterMatchListener listener) {
        List<Event> outgoingEvents = new ArrayList<>();
        for (Event evt : incomingEvents) {
            IngestDocument doc = IngestMarshaller.toDocument(evt);
            IngestDocument result;
            try {
                result = primaryPipeline.execute(doc);
            } catch (Exception ex) {
                // what to do here?
                throw new IllegalStateException(ex);
            }
            if (result != null) {
                Event event = eventFactory.newEvent();
                IngestMarshaller.toEvent(result, event);
                outgoingEvents.add(event);
                listener.filterMatched(event);
            } else {
                evt.cancel();
                outgoingEvents.add(evt);
            }
        }
        return outgoingEvents;
    }

    @Override
    public Collection<Event> flush(FilterMatchListener matchListener) {
        return Collections.emptyList();
    }

    @Override
    public boolean requiresFlush() {
        return false;
    }

    @Override
    public boolean requiresPeriodicFlush() {
        return false;
    }

    @Override
    public Pipeline getPipelineByName(String name) {
        return pipelines.get(name);
    }

    private Pipeline getPipeline(String pipelineId, String json) {
        Pipeline pipeline = null;
        try {
            BytesReference b = new BytesArray(json);
            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(b, false, XContentType.JSON).v2();
            pipeline = Pipeline.create(pipelineId, pipelineConfig, getProcessorFactories(), getScriptService());
        } catch (Exception e) {
            System.out.println("Error building pipeline\n" + e);
            e.printStackTrace();
        }
        return pipeline;
    }

    private Map<String, Processor.Factory> getProcessorFactories() {
        Processor.Parameters processorParameters = getParameters();
        IngestCommonPlugin ingestCommonPlugin = new IngestCommonPlugin();
        Map<String, Processor.Factory> defaultFactories = ingestCommonPlugin.getProcessors(processorParameters);
        Map<String, Processor.Factory> userAgentFactory = new IngestUserAgentPlugin().getProcessors(processorParameters);
        Map<String, Processor.Factory> geoipFactory = new IngestGeoIpPlugin().getProcessors(processorParameters);
        Map<String, Processor.Factory> overriddenFactories = new HashMap<>(defaultFactories);
        overriddenFactories.putAll(userAgentFactory);
        overriddenFactories.putAll(geoipFactory);
        overriddenFactories.put(PipelineProcessor.TYPE, new PipelineProcessor.Factory(this));
        overriddenFactories.put(SetSecurityUserProcessor.TYPE, new SetSecurityUserProcessor.Factory());
        return Collections.unmodifiableMap(overriddenFactories);
    }

    private Processor.Parameters getParameters() {
        final ThreadPool threadPool = new ThreadPool(getSettings());

        BiFunction<Long, Runnable, ScheduledFuture<?>> scheduler =
                (delay, command) -> threadPool.schedule(TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC, command);
        return new Processor.Parameters(getEnvironment(), getScriptService(), null, null,
                threadPool::relativeTimeInMillis, scheduler, null);
    }

    private Settings getSettings() {
        return Settings.builder()
                .put("path.home", "/")
                .put("node.name", nodeName)
                .put("ingest.grok.watchdog.interval", "1s")
                .put("ingest.grok.watchdog.max_execution_time", "1s")
                .put("ingest.geoip.database_path", "local_libs")
                .build();
    }

    private Environment getEnvironment() {
        return new Environment(getSettings(), null);
    }

    private ScriptService getScriptService() {
        Map<String, ScriptEngine> engines = new HashMap<>();
        engines.put(PainlessScriptEngine.NAME, new PainlessScriptEngine(getSettings(), scriptContexts()));
        engines.put(MustacheScriptEngine.NAME, new MustacheScriptEngine());
        return new ScriptService(getSettings(), engines, ScriptModule.CORE_CONTEXTS);
    }

    private static Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(IngestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(IngestConditionalScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        return contexts;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return PluginHelper.commonFilterSettings(Arrays.asList(NODE_NAME, WATCHDOG_INTERVAL, WATCHDOG_MAX_TIME,
                PIPELINE_DEFINITIONS, PRIMARY_PIPELINE));
    }

    @Override
    public String getId() {
        return id;
    }

    private static FileInputStream toFileInputStream(String filename) {
        try {
            return new FileInputStream(filename);
        } catch (FileNotFoundException ex) {
            throw new IllegalArgumentException("Unable to open file '" + filename + "'", ex);
        }
    }
}
