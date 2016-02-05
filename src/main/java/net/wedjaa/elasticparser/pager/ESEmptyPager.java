/****
 *
 * Copyright 2013-2016 Wedjaa <http://www.wedjaa.net/>
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
import java.util.Map;

import org.apache.log4j.Logger;

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
	public long current_hit_idx() {
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
    public long getResultsCount() {
        return 0;
    }

	@Override
	public String get_query() {
		return "";
	}

	@Override
	public Map<String, Object> next() {
		return new HashMap<String, Object>();
	}

	@Override
	public Map<String,Class<?>> getResponseFields() {
		Map<String,Class<?>> result = new HashMap<String,Class<?>>();
		return result;
	}

}
