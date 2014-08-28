# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Blueflood < LogStash::Outputs::Base
  # This output lets you `POST` events to a
  # Blueflood endpoint

  config_name "blueflood"
  milestone 1

  # URL to use
  config :url, :validate => :string, :required => :true
  config :port, :validate => :string	
  config :tenant_id, :validate => :string	
  config :json_metrics, :validate => :string
  config :hash_metrics, :validate => :hash, :default => {}
  config :format, :validate => ["json","hash"], :default => "json"
  
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
	
	if @format == "json"
		if @json_metrics.nil?
			raise "json metrics need to be set since format is json"
		end
		if @original_params["ttl"]
			raise "json metrics string need to contain ttl; it can't be set from the configuration"
		end
	else 
		if @format == "hash"
			if @hash_metrics.nil?
				raise "hash_metrics need to be set with a valid dictionary since format is hash"
			end
		end
	end
  end # def register

  public
  def receive(event)
    return unless output?(event)

    request = @agent.post(event.sprintf(@url))
    request["Content-Type"] = "application/json"
	timestamp = event.sprintf("%{+%s}")
	messages = []

    begin
    	if @format == "json"
			request.body = event.sprintf(@json_metrics)
		else
			@hash_metrics.each do |metric, value|
				 @logger.debug("processing", :metric => metric, :value => value)
				 metric = event.sprintf(metric)
				 jsonstring = '{"collectionTime": %s, "ttlInSeconds": %s, "metricValue": %s, "metricName": "%s"}' % [timestamp,@ttl,event.sprintf(value).to_f,event.sprintf(metric)]
				 messages << jsonstring
				 puts jsonstring
			end
			jsonarray = "[%s]"%messages.join(",")
			request.body = jsonarray
			#request.body = messages.to_json
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
