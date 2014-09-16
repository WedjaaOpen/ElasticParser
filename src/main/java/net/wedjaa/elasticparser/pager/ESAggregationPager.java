package net.wedjaa.elasticparser.pager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.wedjaa.elasticparser.resolver.AggregateResolver;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;

public class ESAggregationPager implements ESResultsPager {

	private Logger logger = Logger.getLogger(ESAggregationPager.class);

	private String query = "";
	private final AggregateResolver aggregateResolver;
	private List<Map<String,Object>> aggregate_values;
	private Iterator<Map<String, Object>> valueIterator;
	private int current_idx = 0;
	
	public ESAggregationPager(SearchResponse initialResponse, String query) {
		
		this.query = query;
		
		this.aggregateResolver= AggregateResolver.getInstance();
		Aggregations aggregations = initialResponse.getAggregations();
		if ( aggregations != null ) {
			logger.debug("Aggregations pager is being populated");
	        this.aggregate_values = aggregateResolver.explode(aggregations);

		} else {
			logger.warn("Aggregation pager not being populated: no aggregations have been found in the result");
			this.aggregate_values = new ArrayList<Map<String,Object>>();
		}
		
		this.valueIterator = this.aggregate_values.iterator();
	}
	
	@Override
	public boolean done() {
		return !valueIterator.hasNext();
	}

	@Override
	public boolean hit_available() {
		return valueIterator.hasNext();
	}

	@Override
	public void set_page_size(int page_size) {
	}

	@Override
	public int current_hit_idx() {
		return current_idx;
	}

	@Override
	public int page_size() {
		return aggregate_values.size();
	}

	@Override
	public int next_page() {
		return 0;
	}

	@Override
	public String get_query() {
		return query;
	}

	@Override
	public Map<String, Object> next(SearchResponse response) {
		current_idx++;
		return valueIterator.next();
	}

	@Override
	public Map<String,Class<?>> getResponseFields() {
		Map<String,Class<?>> result = new HashMap<String,Class<?>>();
		
		if ( aggregate_values != null ) {
			for (Map<String,Object> value: aggregate_values) {
				for (String fieldName: value.keySet() ) {
					if ( !result.containsKey(fieldName)) {
						result.put(fieldName, value.get(fieldName).getClass());
					}
				}
			}
		}

		return result;
	}
	
}
