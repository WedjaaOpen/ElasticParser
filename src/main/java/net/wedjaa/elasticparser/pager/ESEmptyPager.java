package net.wedjaa.elasticparser.pager;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;

public class ESEmptyPager implements ESResultsPager {

	private Logger logger = Logger.getLogger(ESEmptyPager.class);
	
	public ESEmptyPager() {
		logger.warn("Returning an empty pager - check the query mode!");
	}
	
	@Override
	public boolean done() {
		return true;
	}

	@Override
	public boolean hit_available() {
		return false;
	}

	@Override
	public void set_page_size(int page_size) {
	}

	@Override
	public int current_hit_idx() {
		return 0;
	}

	@Override
	public int page_size() {
		return 0;
	}

	@Override
	public int next_page() {
		return 0;
	}

	@Override
	public String get_query() {
		return "";
	}

	@Override
	public Map<String, Object> next(SearchResponse response) {
		return new HashMap<String, Object>();
	}

	@Override
	public Map<String,Class<?>> getResponseFields() {
		Map<String,Class<?>> result = new HashMap<String,Class<?>>();
		return result;
	}
	
}
