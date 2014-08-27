# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Http < LogStash::Outputs::Base
  # This output lets you `POST` events to a
  # Blueflood endpoint

  config_name "blueflood"
  milestone 1

  # URL to use
  config :url, :validate => :string, :required => :true

  config :content_type, :validate => :string, :default => "application/json"
  
  config :port, :validate => :string	
  config :tenant_id, :validate => :string	
  config :metrics, :validate => :string
  config :hash_metrics, :validate => :hash, :default => {}
  config :format, :validate => ["json","hash"], :default => "json"

  public
  def register
    require "ftw"
    require "uri"
    require "json"

    @agent = FTW::Agent.new
    @url = url+":"+port+"/v2.0/"+tenant_id+"/ingest"
    @content_type = "application/json"
  end # def register

  public
  def receive(event)
    return unless output?(event)

    request = @agent.post(event.sprintf(@url))
    request["Content-Type"] = @content_type
	timestamp = event.sprintf("%{+%s}")
	messages = []

    begin
    	if @format == "json"
			puts @metrics
			request.body = event.sprintf(@metrics)
		else
			@hash_metrics.each do |metric, value|
				 @logger.debug("processing", :metric => metric, :value => value)
				 metric = event.sprintf(metric)
				 jsonstring = '{"collectionTime": %s, "ttlInSeconds": 172800, "metricValue": %s, "metricName": "%s"}'% [timestamp,event.sprintf(value).to_f,event.sprintf(metric)]
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
