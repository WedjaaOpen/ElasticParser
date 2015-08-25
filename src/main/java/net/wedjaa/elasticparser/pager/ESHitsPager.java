/****
 *
 * Copyright 2013-2014 Wedjaa <http://www.wedjaa.net/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

/**
 * @author Fabio Torchetti
 *
 */

package net.wedjaa.elasticparser.pager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

public class ESHitsPager implements ESResultsPager {

	/**
	 * This is the number of results per shard we are going to get by default.
	 */
	public final static int PAGE_SIZE = 100;

	// Time to keep the scrollid active: 1hr
	public final static long SCROLL_KEEPALIVE = 3600000;
	private Iterator<SearchHit> hits;
	private final Client esClient;
	private long total_hits = 0;
	private int page = 0;
	private int page_size = 0;
	private long hits_count = 0;
	private String query;
	private SearchResponse searchResponse;
	private Logger logger = Logger.getLogger(ESHitsPager.class);

	public ESHitsPager(SearchResponse searchResponse, String query, int page_size, Client esClient) {
		this.total_hits = searchResponse.getHits().totalHits();
		this.hits_count = 0;
		/**
		 * This page size is here waiting for major refactoring to happen. It
		 * needs to be removed, since with scrolling the page size needs to be
		 * multiplied by the shards to get an idea of the number of results we
		 * will receive.
		 */
		this.page_size = page_size;
		this.query = query;
		this.page = -1;
		this.searchResponse = searchResponse;
		this.hits = searchResponse.getHits().iterator();
		this.esClient = esClient;
	}

	public int next_page() {
		page++;
		return page * page_size;
	}

	public boolean hit_available() {
		return hits_count < total_hits;
	}

	public long current_hit_idx() {
		return hits_count;
	}

	public int next_hit_idx() {
		hits_count++;
		return (int) hits_count;
	}

	public int page_size() {
		return page_size;
	}

	@Override
	public long getResultsCount() {
		return total_hits;
	}

	public void set_page_size(int page_size) {
		this.page = 0;
		this.page_size = page_size;
	}

	public boolean done() {
		logger.trace("Checking if done: current hits: " + hits_count + " of " + total_hits);
		return hits_count > total_hits;
	}

	public String get_query() {
		return query;
	}

	@Override
	public Map<String, Object> next() {

		// Get the next page_size of results if we have exhausted the
		// current list.
		if (!hits.hasNext()) {
			try {
				logger.debug("Using ScrollID: " + searchResponse.getScrollId());
				searchResponse = esClient.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(SCROLL_KEEPALIVE)).execute().get();
				logger.debug("Got another " + searchResponse.getHits().getHits().length + " results.");
			} catch (Exception ex) {
				logger.warn("Failed to get the next bunch of results! [" + ex.getMessage() + "]");
				return null;
			}

			hits = searchResponse.getHits().iterator();
		}

		if (!hits.hasNext()) {
			// Nothing else to return, the search result was empty!
			return null;
		}

		hits_count++;
		SearchHit hit = hits.next();
		if(hit.getSource() != null) {
			return hit.getSource();
		}
		if(hit.fields() != null) {
			Map<String, Object> fields = new HashMap<>();
			for(Map.Entry<String, SearchHitField> field : hit.fields().entrySet()) {
				fields.put(field.getKey(), field.getValue().getValue());
			}
			return fields;
		}
		return null;
	}

	@Override
	public Map<String, Class<?>> getResponseFields() {
		logger.debug("Hit Parser - Getting fields");

		Map<String, Class<?>> result = new HashMap<String, Class<?>>();

		/**
		 * We have not been properly initialized, return an empty set of fields.
		 */
		if (searchResponse == null) {
			logger.warn("Can't return fields for empty search!");
			return result;
		}

		/**
		 * The searchResponse we have received when initialized is an empty one,
		 * we need to run the first search to get results from the scroll.
		 */
		try {
			logger.debug("Getting the first scroll results");
			searchResponse = esClient.prepareSearchScroll(searchResponse.getScrollId())
					.setScroll(new TimeValue(SCROLL_KEEPALIVE)).execute().get();
		} catch (Exception ex) {
			/**
			 * Return an empty set of fields in case of errors.
			 */
			logger.warn("Error fetching results for fields: " + ex.getLocalizedMessage());
			return result;
		}

		if (searchResponse.getHits() != null) {
			logger.debug("Response has hits...");
			SearchHit[] hits = searchResponse.getHits().getHits();
			logger.debug("Hits on this page: " + hits.length);
			for (SearchHit hit : hits) {
				try {
					if(hit.getSource() != null) {
						Set<String> field_names = hit.getSource().keySet();
						for (String field_name : field_names) {
							if (!result.containsKey(field_name)) {
								if (hit.getSource().get(field_name) != null) {
									result.put(field_name, hit.getSource().get(field_name).getClass());
								}
							}
						}
					} else if(hit.fields() != null) {
						for(Map.Entry<String, SearchHitField> field : hit.fields().entrySet()) {
							if(!result.containsKey(field.getKey())) {
								if(field.getValue().getValue() != null) {
									result.put(field.getKey(), field.getValue().getValue().getClass());
								}
							}
						}
					}
				} catch (Exception ex) {
					logger.warn("Exception while handling hit:\n" + hit.getSourceAsString() + "\nError: "
							+ ex.getMessage());
				}
			}

		}
		
		return result;
	}

}
