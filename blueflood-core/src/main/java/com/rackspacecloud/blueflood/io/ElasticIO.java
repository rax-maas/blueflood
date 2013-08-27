package com.rackspacecloud.blueflood.io;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.rackspacecloud.blueflood.service.Configuration;

import static com.rackspacecloud.blueflood.io.ElasticIO.ESKeys.*;


public class ElasticIO {
	private static final Logger log = Logger.getLogger(ElasticIO.class);
	private static Client client;
	// currently not that useful at the moment.
	private static final String ES_TYPE = "metrics";
	
	static {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.ignore_cluster_name", true)
				.build();
		client = new TransportClient(settings)
		.	addTransportAddress(new InetSocketTransportAddress(
				Configuration.getStringProperty("ELASTIC_HOST"), Configuration.getStringProperty("ELASTIC_PORT")));
	}

	static enum ESKeys {
		TENANT_ID,
		LOCATOR;
	}

	public static class InsertRequest {
		public static class Builder {
			private Map<String, Object> annotation = new HashMap<String, Object>();
			private final String locator;

			public Builder(String locator) {
				this.locator = locator;
			}

			public InsertRequest build() {
				return new InsertRequest(this);
			}

			public Builder withAnnotation(Map<String, Object> annotation) {
				this.annotation = annotation;
				return this;
			}
		}

		private final String locator;
		private final Map<String, Object> annotation;

		public InsertRequest(Builder builder) {
			this.locator = builder.locator;
			this.annotation = builder.annotation;
		}

		public Map<String, Object> getAnnotation() {
			return annotation;
		}

		public String getLocator() {
			return locator;
		}

		@Override
		public String toString() {
			return "InsertRequest [locator=" + locator + ", annotation="
					+ annotation + "]";
		}
	}

	public static class SearchRequest {
		public static class Builder {
			private String locatorQuery = "";
			private Map<String, Object> annotationQuery = new HashMap<String, Object>();

			public Builder locatorQuery(String query) {
				this.locatorQuery = query;
				return this;
			}

			public Builder annotationQuery(Map<String, Object> query) {
				this.annotationQuery = query;
				return this;
			}

			public SearchRequest build() {
				return new SearchRequest(this);
			}
		}

		private final String locatorQuery;
		private final Map<String, Object> annotationQuery;

		public SearchRequest(Builder builder) {
			this.locatorQuery = builder.locatorQuery;
			this.annotationQuery = builder.annotationQuery;
		}

		public String getLocatorQuery() {
			return locatorQuery;
		}

		public Map<String, Object> getAnnotationQuery() {
			return annotationQuery;
		}

		@Override
		public String toString() {
			return "SearchRequest [locatorQuery=" + locatorQuery
					+ ", annotationQuery=" + annotationQuery + "]";
		}

	}

	public Boolean insert(final String tenantId, final InsertRequest request) {
		XContentBuilder content;
		try {
			content = createSourceContent(tenantId, request);
		} catch (IOException ie) {
			return false;
		}
		IndexResponse indexRes = client.prepareIndex(getIndex(tenantId), ES_TYPE)
				//.setId(getId(content))
				.setId(getId(tenantId, request))
				.setRouting(getRouting(tenantId))
				.setSource(content)
				//.setVersion(1)
				//.setVersionType(VersionType.EXTERNAL)
				.execute()
				.actionGet();
		log.trace("index=" + indexRes.getIndex() + " id=" + indexRes.getId() + " version=" + indexRes.getVersion());
		return true;
	}
	
	public List<String> getAllLocators(final String tenantId, final SearchRequest query) {
		List<String> matched = new ArrayList<String>();
		String queryString = createQueryString(tenantId, query);
		SearchResponse searchRes = client.prepareSearch(getIndex(tenantId))
				.setSize(500)
				.setRouting(getRouting(tenantId))
				.setVersion(true)
				.setQuery(QueryBuilders.queryString(queryString))
				.execute()
				.actionGet();
		log.trace("querystring="+queryString);
		for (SearchHit hit : searchRes.getHits().getHits()) {
			log.trace("id=" + hit.getId() + ", shard=" + hit.getShard() + ", version=" + hit.version());
			Map<String, Object> result = hit.getSource();
			matched.add((String) result.get(LOCATOR.toString()));
		}
		return matched;
	}
	
	private String getIndex(String tenantId) {
		return "test-index-" + String.valueOf(Math.abs(tenantId.hashCode() % 128));
	}

	/** All requests from the same tenant should go to the same shard.
	 * @param tenantId
	 * @return 
	 */
	private String getRouting(String tenantId) {
		return tenantId;
	}

	private String getId(String tenantId, InsertRequest request) {
		return tenantId + request.toString();
	}
	private String createQueryString(String tenantId, SearchRequest query) {
		StringBuilder builder = new StringBuilder();
		builder.append(TENANT_ID.toString() + ":" + tenantId);

		if (!query.getLocatorQuery().equals("")) {
			builder.append(" && ");
			builder.append(LOCATOR.toString() + ":" + query.getLocatorQuery());
		}

		for (Map.Entry<String, Object> entry : query.getAnnotationQuery().entrySet()) {
			builder.append(" && ");
			builder.append(entry.getKey() + ":" + entry.getValue());
		}
		return builder.toString();
	}
	private XContentBuilder createSourceContent(String tenantId, InsertRequest request) throws IOException {
		XContentBuilder json = XContentFactory.jsonBuilder().startObject();

		json = json.field(TENANT_ID.toString(), tenantId);
		json = json.field(LOCATOR.toString(), request.getLocator());

		for (Map.Entry<String, Object> entry : request.getAnnotation().entrySet()) {
			json = json.field(entry.getKey(), entry.getValue());
		}
		json = json.endObject();
		return json;
	}
}
