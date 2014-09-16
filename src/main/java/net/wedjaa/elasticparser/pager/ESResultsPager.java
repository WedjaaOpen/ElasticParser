package net.wedjaa.elasticparser.pager;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;

public interface ESResultsPager {
	public boolean done();
	public boolean hit_available();
	public void set_page_size(int page_size);
	public int current_hit_idx();
	public int page_size();
	public int next_page();
	public String get_query();
	public Map<String, Object> next(SearchResponse response);
	public Map<String, Class<?>> getResponseFields();
}
