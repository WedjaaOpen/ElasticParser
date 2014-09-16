package net.wedjaa.elasticparser.pager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.wedjaa.elasticparser.resolver.FacetResolver;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.facet.Facets;

public class ESFacetsPager implements ESResultsPager {

	private Logger logger = Logger.getLogger(ESFacetsPager.class);

	private String query = "";
	private final FacetResolver facetResolver;
	private List<Map<String,Object>> facet_values;
	private Iterator<Map<String, Object>> valueIterator;
	private int current_idx = 0;
	
	public ESFacetsPager(SearchResponse initialResponse, String query) {
		
		this.query = query;
		
		this.facetResolver= FacetResolver.getInstance();
		Facets facets = initialResponse.getFacets();
		if ( facets != null ) {
			logger.debug("Facets pager is being populated");
	        this.facet_values = facetResolver.explode(facets);

		} else {
			logger.warn("Facets pager not being populated: no facets have been found in the result");
			this.facet_values = new ArrayList<Map<String,Object>>();
		}
		
		this.valueIterator = this.facet_values.iterator();
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
		return facet_values.size();
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
		
		if ( facet_values != null ) {
			for (Map<String,Object> value: facet_values) {
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
