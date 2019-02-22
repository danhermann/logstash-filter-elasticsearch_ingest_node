PLUGIN_VERSION = File.read(File.expand_path(File.join(File.dirname(__FILE__), "VERSION"))).strip unless defined?(PLUGIN_VERSION)

Gem::Specification.new do |s|
  s.name            = 'logstash-filter-elasticsearch_ingest_node'
  s.version         = PLUGIN_VERSION
  s.licenses        = ['Apache-2.0']
  s.summary         = 'Runs Elasticsearch ingest node pipelines in Logstash'
  s.description     = 'This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program'
  s.authors         = ['Elasticsearch']
  s.email           = ['info@elastic.co']
  s.homepage        = 'http://www.elastic.co/guide/en/logstash/current/index.html'
  s.require_paths = ['lib', 'vendor/jar-dependencies']

  s.files = Dir["lib/**/*","*.gemspec","*.md","CONTRIBUTORS","Gemfile","LICENSE","NOTICE.TXT", "vendor/jar-dependencies/**/*.jar", "vendor/jar-dependencies/**/*.rb", "VERSION", "docs/**/*"]

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { 'logstash_plugin' => 'true', 'logstash_group' => 'filter'}

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency 'jar-dependencies'
  s.add_development_dependency 'logstash-devutils'
end
