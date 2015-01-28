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

package net.wedjaa.elasticparser.tester;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.wedjaa.elasticparser.ESSearch;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.json.JSONObject;

public class ESSearchTester {

	private static String getQuery(String queryName) {
		
		StringBuffer sb = new StringBuffer();

		BufferedReader br;
		
		try {
			br = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(queryName), "UTF-8"));
			for (int c = br.read(); c != -1; c = br.read()) sb.append((char)c);
		} catch (Exception e) {
			System.err.println("Failed to read query: " + e);
		}

		String queryString = sb.toString();
		System.out.println("Loaded Query: " + queryString);
		return queryString; 	
	}
	
	private static int countIndexed() {
		System.out.println("Getting the indexing status");
		int indexed = 0;
		
		HttpGet statusGet = new HttpGet("http://localhost:9500/unit/_stats");
		try {
			CloseableHttpClient httpclient = HttpClientBuilder.create().build();
			System.out.println("Executing request");
			HttpResponse response = httpclient.execute(statusGet);
			System.out.println("Processing response");
			InputStream isResponse = response.getEntity().getContent();
	        BufferedReader isReader = new BufferedReader(new InputStreamReader(isResponse));
	        StringBuilder strBuilder = new StringBuilder();
	        String readLine;
	        while ((readLine = isReader.readLine()) != null) {
	        	strBuilder.append(readLine);
	        }			
	        isReader.close();
	        System.out.println("Done - reading JSON");
			JSONObject esResponse = new JSONObject(strBuilder.toString());
			indexed = esResponse.getJSONObject("indices")
					.getJSONObject("unit").getJSONObject("total").getJSONObject("docs").getInt("count");
			System.out.println("Indexed docs: " + indexed);
			httpclient.close();
		} catch (ClientProtocolException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}	
		
		return indexed;
	}
	
	private static void populateTest() {
		// Get the bulk index as a stream
		InputStream bulkStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("bulk-insert.json");
	    CloseableHttpClient httpclient = HttpClientBuilder.create().build();
	    
	    HttpResponse response;
	    // Drop the index if it's there
	    HttpDelete httpdelete = new HttpDelete("http://localhost:9500/unit");
	    try {
			response = httpclient.execute(httpdelete);
			System.out.println("Index Deleted: " + response);
			httpclient.close();
		} catch (ClientProtocolException protoException) {
			System.err.println("Protocol Error while deleting index: " + protoException);
		} catch (IOException ioException) {
			System.err.println("IO Error while deleting index: " + ioException);
		}
	    
	    HttpPost httppost = new HttpPost("http://localhost:9500/_bulk");
	    
	    InputStreamEntity isEntity = new InputStreamEntity(bulkStream);
	    httppost.setEntity(isEntity);

		try {
			httpclient = HttpClientBuilder.create().build();
			response = httpclient.execute(httppost);
		    System.out.println(response.getStatusLine());
		    httpclient.close();
		} catch (ClientProtocolException protoException) {
			System.err.println("Protocol Error while bulk indexing: " + protoException);
		} catch (IOException ioException) {
			System.err.println("IO Error while bulk indexing: " + ioException);
		}
		System.out.println("Waiting for index to settle down...");
		while ( countIndexed() < 50 ) {
			System.out.println("...");
		}
		System.out.println("...done!");
	}
	
	public static void main(String[] args) {
		
		System.out.println("ES Search Testing started.");
	
		
		System.out.println("Starting local node...");
		
		Settings nodeSettings = ImmutableSettings.settingsBuilder()
					.put("transport.tcp.port", "9600-9700")
					.put("http.port", "9500")
					.put("http.max_content_length", "104857600")
					.build();
		
		Node node = NodeBuilder.nodeBuilder()
					.settings(nodeSettings)
					.clusterName("elasticparser.unittest")
					.node();
		
		node.start();
		
		// Populate our test index
		System.out.println("Preparing Unit Test Index - this may take a while...");
		populateTest();
		
		try {
		System.out.println("...OK - Executing query!");
		// Try our searches
		ESSearch search = new ESSearch(null, null, ESSearch.ES_MODE_AGGS, "localhost", 9600, "elasticparser.unittest" );
		search.search(getQuery("test-aggs.json"));
		Map<String, Object> hit = null;
		while (  (hit = search.next()) != null ) {
			System.out.println("Hit: {");
			for ( String key: hit.keySet() ) {
				System.out.println("  "+key+": " + hit.get(key));
			}
			System.out.println("};");
		}
		search.close();
		Map<String, Class<?>> fields = search.getFields(getQuery("test-aggs.json"));
		List<String> sortedKeys=new ArrayList<String>(fields.keySet());
		Collections.sort(sortedKeys);
		Iterator<String> sortedKeyIter = sortedKeys.iterator();
		while ( sortedKeyIter.hasNext() ) {
			String fieldname = sortedKeyIter.next();
			System.out.println(" --> " + fieldname + "["+ fields.get(fieldname).getCanonicalName() +"]");
		}
		
		} catch(Exception ex) {
			System.out.println("Exception: " + ex);
		} finally {
			System.out.println("Stopping Test Node");
			node.stop();
		}
	}

}
