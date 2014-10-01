# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Blueflood < LogStash::Outputs::Base
  # This output lets you pull metrics from your logs and emit them
  # to your Blueflood endpoint. 
  #
  # Blueflood (http://blueflood.io) is a distributed, multi-tenant
  # metrics processing solution, created by engineers at Rackspace
  # Blueflood is capable of ingesting, rolling up and serving metrics 
  # at a massive scale. 
  #
  # The metrics can either be sent as well-formed JSON 
  # (examples: https://github.com/rackerlabs/blueflood/wiki/10minuteguide)
  # or as a hash. 
  # 
  # Sample use case: You are logging the load average of your system every 30 seconds
  # Using a grok filter and the Blueflood output plugin, you can send these metrics to
  # Blueflood. See many such examples in our sample logstash conf files here
  # https://github.com/rackerlabs/blueflood/tree/master/contrib/logstash-blueflood
  
  config_name "blueflood"
  milestone 1

  # This setting is the url of your Blueflood instance
  # Sample value: http://127.0.0.1
  config :url, :validate => :string, :default => "localhost"
  
  # This setting is the port at which Blueflood listens for ingest requests.
  # Sample value: 19000
  config :port, :validate => :string, :default => "19000"
  
  # This setting is the id of the tenant for which you are sending metrics
  # Blueflood is a multi-tenant metrics store. 
  # Sample Value: My company  name ie tgCompany
  config :tenant_id, :validate => :string, :required => true

  # This setting is used to send well formed json that Blueflood expects
  # Sample Value: '[{"collectionTime": 1376509892612, "ttlInSeconds": 172800, "metricValue": 66, "metricName":"example.metric.one"}]'
  # See usage in conf file https://github.com/rackerlabs/blueflood/tree/master/contrib/logstash-blueflood/blueflood.conf
  # Either this or :hash_metrics is required
  config :json_metrics, :validate => :string
  
  # This setting is used to send metrics as a hash of key value pairs
  # Sample Value:  [ "hosts.%{@source_host}.load_avg.1m", "%{load_avg_1m}"]
  # See usage in conf file https://github.com/rackerlabs/blueflood/tree/master/contrib/logstash-blueflood/blueflood-hash-metrics.conf
  # Either this or :json_metrics is required
  config :hash_metrics, :validate => :hash
  
  # Cassandra TTL, (in seconds,) to use. Only works with hash_metrics.
  # If you are using json_metrics, that string will need to include the
  # ttlInSeconds.
  config :ttl, :validate => :number, :default => 172800

  public
  def register
    require "ftw"
    require "uri"
    require "json"

    @agent = FTW::Agent.new
    @url = "%s:%s/v2.0/%s/ingest"%[@url,@port,@tenant_id]
    
    if (@json_metrics && @hash_metrics) || (@json_metrics.nil? && @hash_metrics.nil?)
      raise "exactly one of json_metrics and hash_metrics must be set."
    end
    
    if @json_metrics
      @format = "json"
    else
      @format = "hash"
    end
    
    if @format == "json" &&  @original_params["ttl"]
      raise "json metrics string need to contain ttl; it can't be set from the configuration."
    end
  end # def register

  public
  def receive(event)
    return unless output?(event)

    request = @agent.post(event.sprintf(@url))
    request["Content-Type"] = "application/json"
    timestamp = event.sprintf("%{+%s}")
    messages = []
    include_metrics = ["-?\\d+(\\.\\d+)?"] #only numeric metrics for now 
    include_metrics.collect!{|regexp| Regexp.new(regexp)}

    begin
      if @format == "json"
        request.body = event.sprintf(@json_metrics)
      else
        @hash_metrics.each do |metric, value|
          @logger.debug("processing", :metric => metric, :value => value)
          metric = event.sprintf(metric)
          next unless include_metrics.empty? || include_metrics.any? { |regexp| value.match(regexp) }
          jsondata = {collectionTime: timestamp.to_i, ttlInSeconds: @ttl.to_i, metricValue: event.sprintf(value).to_f, metricName: event.sprintf(metric)}

          messages << jsondata
        end
        request.body = messages.to_json
      end
      response = @agent.execute(request)
      
      # Consume body to let this connection be reused
      rbody = ""
      response.read_body { |c| rbody << c }
      puts rbody
    rescue Exception => e
      @logger.error("Unhandled exception", :request => request.body, :response => response)#, :exception => e, :stacktrace => e.backtrace)
    end
  end # def receive
end
