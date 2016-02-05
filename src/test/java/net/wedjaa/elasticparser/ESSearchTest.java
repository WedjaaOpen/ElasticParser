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

package net.wedjaa.elasticparser;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by mrwho on 17/03/15.
 */

public class ESSearchTest
{

    static final int GENERAL_NUM_HITS = 26;
    static final int GENERAL_NUM_FIELDS = 36;
    static final int PROJECTION_NUM_FIELDS = 3;
    static final int GENERAL_NUM_AGGS = 16;

    static final int MULTIPLE_NUM_AGGS = 3;
    static final double MULTIPLE_AGGS_UTIL_STAT_SUM = 124;
    static final double MULTIPLE_AGGS_UTIL_NUM_SUM = 43;
    static final double MULTIPLE_AGGS_PRAG_STAT_SUM = 35;
    static final double MULTIPLE_AGGS_PRAG_NUM_SUM = 14;

    static final double GENERAL_AGGS_FIRST_TERM_COUNT = 26;
    static final double GENERAL_AGGS_SECOND_TERM_COUNT = 20;
    static final int TEST_TYPE_NUM_HITS = 20;
    static final int TEST_TYPE_NUM_FIELDS = 7;
    static final int LARGE_NUM_HITS = 1;
    static final int LARGE_NUM_FIELDS = 28;
    static final int MULTIPLE_TYPES_NUM_HITS = 25;
	private static final String SIMPLE_AGG_NAME = "total";
	private static final Long SIMPLE_AGG_COUNT = (long) 20;

    static Logger logger = Logger.getLogger(ESSearchTest.class);

    static Node elasticTestNode;
    static String clusterName;

    @BeforeClass public static void setup()
    {
        clusterName = java.util.UUID.randomUUID().toString();
        logger.info("Starting ES test node!");
        elasticTestNode = startESNode(clusterName);
        logger.info("Populating test data");
        populateTest();
    }

    @AfterClass public static void teardown()
    {
        logger.info("Stopping ES test node");
        stopNode(elasticTestNode);
    }

    @Test
    public void testSingleType()
    {
      int hitCount = 0;

      logger.info("Testing Single Type");
      // Try our searches
      ESSearch search = new ESSearch("unit", "test", ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
      search.search(getQuery("test-hits.json"));
      while ((search.next()) != null)
      {
          hitCount++;
      }
      search.close();
      Assert.assertEquals("Single type number of hits", TEST_TYPE_NUM_HITS, hitCount);

    }

    @Test
    public void testMultipleTypes()
    {
      int hitCount = 0;

      logger.info("Testing Multiple Types");
      // Try our searches
      ESSearch search = new ESSearch("unit", "test,related", ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
      search.search(getQuery("test-hits.json"));
      while ((search.next()) != null)
      {
          hitCount++;
      }
      search.close();
      Assert.assertEquals("Multiple types number of hits", MULTIPLE_TYPES_NUM_HITS, hitCount);

    }

    @Test
    public void testLargeHitDocument()
    {
      int hitCount = 0;

      logger.info("Testing Large Document");
      // Try our searches
      ESSearch search = new ESSearch("unit", "large", ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
      search.search(getQuery("test-hits.json"));
      while ((search.next()) != null)
      {
          hitCount++;
      }
      search.close();
      Assert.assertEquals("Large document number of hits", LARGE_NUM_HITS, hitCount);

    }

    @Test
    public void testLargeHitFields()
    {
        logger.info("Testing Large Hit Fields");
        ESSearch search = new ESSearch("unit", "large", ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
        Map<String, Class<?>> fields = search.getFields(getQuery("test-hits.json"));
        List<String> sortedKeys = new ArrayList<String>(fields.keySet());
        Collections.sort(sortedKeys);
        Iterator<String> sortedKeyIter = sortedKeys.iterator();
        int fieldsCount = 0;
        while (sortedKeyIter.hasNext())
        {
            String fieldname = sortedKeyIter.next();
            logger.debug(" --> " + fieldname + "[" + fields.get(fieldname).getCanonicalName() + "]");
            fieldsCount++;
        }
        search.close();
        Assert.assertEquals("Number of total fields", LARGE_NUM_FIELDS, fieldsCount);
    }

    @Test
    public void testSinglePageHits()
    {
        int hitCount = 0;

        logger.info("Testing Hits Mode");
        // Try our searches
        ESSearch search = new ESSearch(null, null, ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
        search.search(getQuery("test-hits.json"));
        while ((search.next()) != null)
        {
            hitCount++;
        }
        search.close();

        Assert.assertEquals("Single page number of hits", GENERAL_NUM_HITS, hitCount);
    }

    @Test
    public void testMultiPageHits()
    {

        int hitCount = 0;

        logger.info("Testing Multi Page Hits Mode");
        // Try our searches
        ESSearch search = new ESSearch(null, null, ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
        /**
         * 10 (2 times 5 shards) will break the results in 3 pages.
         */
        String sizedQuery = setQuerySize(getQuery("test-hits.json"), 2);
        search.search(sizedQuery);
        Map<String, Object> hit = search.next();
        while (hit != null)
        {
            hitCount++;
            hit = search.next();
        }
        search.close();

        Assert.assertEquals("Multi page number of hits", GENERAL_NUM_HITS, hitCount);
    }

    @Test
    public void testHitsFields()
    {
        logger.info("Testing Generic Hits Fields");
        ESSearch search = new ESSearch(null, null, ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
        Map<String, Class<?>> fields = search.getFields(getQuery("test-hits.json"));
        List<String> sortedKeys = new ArrayList<String>(fields.keySet());
        Collections.sort(sortedKeys);
        Iterator<String> sortedKeyIter = sortedKeys.iterator();
        int fieldsCount = 0;
        while (sortedKeyIter.hasNext())
        {
            String fieldname = sortedKeyIter.next();
            logger.debug(" --> " + fieldname + "[" + fields.get(fieldname).getCanonicalName() + "]");
            fieldsCount++;
        }
        search.close();
        Assert.assertEquals("Number of total fields", GENERAL_NUM_FIELDS, fieldsCount);
    }

    @Test
    public void testHitsFieldsWithProjection()
    {
        logger.info("Testing Hits Fields with Projection");
        ESSearch search = new ESSearch(null, null, ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
        Map<String, Class<?>> fields = search.getFields(getQuery("test-hits-projection.json"));
        List<String> sortedKeys = new ArrayList<String>(fields.keySet());
        Collections.sort(sortedKeys);
        Iterator<String> sortedKeyIter = sortedKeys.iterator();
        int fieldsCount = 0;
        while (sortedKeyIter.hasNext())
        {
            String fieldname = sortedKeyIter.next();
            logger.debug(" --> " + fieldname + "[" + fields.get(fieldname).getCanonicalName() + "]");
            fieldsCount++;
        }
        search.close();
        Assert.assertEquals("Number of total fields projected", PROJECTION_NUM_FIELDS, fieldsCount);
    }

    @Test
    public void testTypeTestHits()
    {
        int hitCount = 0;

        logger.info("Testing Type Test Hits Mode");
        // Try our searches
        ESSearch search = new ESSearch(null, "test", ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
        search.search(getQuery("test-hits.json"));
        Map<String, Object> hit = search.next();
        while (hit != null)
        {
            hitCount++;
            hit = search.next();
        }
        search.close();

        Assert.assertEquals("Number of hits for type.", TEST_TYPE_NUM_HITS, hitCount);
    }

    @Test
    public void testTypeTestFields()
    {
        logger.info("Testing Type Test Hits Fields");
        ESSearch search = new ESSearch(null, "test", ESSearch.ES_MODE_HITS, "localhost", 9600, clusterName);
        Map<String, Class<?>> fields = search.getFields(getQuery("test-hits.json"));
        List<String> sortedKeys = new ArrayList<String>(fields.keySet());
        Collections.sort(sortedKeys);
        Iterator<String> sortedKeyIter = sortedKeys.iterator();
        int fieldsCount = 0;
        while (sortedKeyIter.hasNext())
        {
            String fieldname = sortedKeyIter.next();
            logger.debug(" --> " + fieldname + "[" + fields.get(fieldname).getCanonicalName() + "]");
            fieldsCount++;
        }
        search.close();
        Assert.assertEquals("Number of fields for type.", TEST_TYPE_NUM_FIELDS, fieldsCount);
    }


    @Test
    public void testNestedAggregations()
    {
        logger.info("Testing Nested Aggregations");
        ESSearch search = new ESSearch(null, null, ESSearch.ES_MODE_AGGS, "localhost", 9600, clusterName);
        search.search(getQuery("test-aggs.json"));
        Map<String, Object> hit;
        int aggsCount = 0;
        while ((hit = search.next()) != null)
        {
            logger.debug("Aggregate: " + hit);

            if (hit.get("Aggregation").toString().equals("People Groups") &&
                    hit.get("People Groups Key").toString().equals("utilitarians") &&
                    hit.get("Group Number Key").toString().equals("5"))
            {
                Assert.assertEquals("The sum of the utilitarians in group 5 should be " + GENERAL_AGGS_FIRST_TERM_COUNT,
                        GENERAL_AGGS_FIRST_TERM_COUNT, hit.get("Group Stats Sum"));
            }
            if (hit.get("Aggregation").toString().equals("People Groups") &&
                    hit.get("People Groups Key").toString().equals("practicals")  &&
                    hit.get("Group Number Key").toString().equals("6"))
            {
                Assert.assertEquals("The sum of the practicals in group 6 should be " + GENERAL_AGGS_SECOND_TERM_COUNT,
                        GENERAL_AGGS_SECOND_TERM_COUNT, hit.get("Group Stats Sum"));
            }
            aggsCount++;
        }
        search.close();
        Assert.assertEquals("Nested Aggregations Return " + GENERAL_NUM_AGGS + " Rows",GENERAL_NUM_AGGS, aggsCount);
    }

    @Test
    public void testSingleAggregation()
    {
        logger.info("Testing Single Aggregations");
        ESSearch search = new ESSearch(null, null, ESSearch.ES_MODE_AGGS, "localhost", 9600, clusterName);
        search.search(getQuery("test-simple-aggs.json"));
        Map<String, Object> hit = search.next();
        Assert.assertNotNull(hit);
        Assert.assertEquals(SIMPLE_AGG_NAME, hit.get("Aggregation"));
        Assert.assertEquals(SIMPLE_AGG_COUNT, hit.get("total DocCount"));
        Assert.assertNull(search.next());
        search.close();
    }


    @Test
    public void testMultipleAggregations()
    {
        logger.info("Testing Multiple Aggregations");
        ESSearch search = new ESSearch(null, null, ESSearch.ES_MODE_AGGS, "localhost", 9600, clusterName);
        search.search(getQuery("test-multi-aggs.json"));
        Map<String, Object> hit;
        int aggsCount = 0;
        while ((hit = search.next()) != null)
        {
            logger.debug("Aggregate: " + hit);
            if (
                    hit.get("Aggregation").toString().equals("People Groups")
                            &&
                            hit.get("People Groups Key").toString().equals("utilitarians")
                    ) {
                        Assert.assertEquals("Utilitarian Stats Sum", MULTIPLE_AGGS_UTIL_STAT_SUM, hit.get("General Stats Sum"));
                        Assert.assertEquals("Utilitarian Numbers Sum",MULTIPLE_AGGS_UTIL_NUM_SUM, hit.get("Number Stats Sum"));
            }
            if (
                    hit.get("Aggregation").toString().equals("People Groups")
                            &&
                            hit.get("People Groups Key").toString().equals("pragmatist")
                    ) {
                Assert.assertEquals("Pragmatist Stats Sum", MULTIPLE_AGGS_PRAG_STAT_SUM, hit.get("General Stats Sum"));
                Assert.assertEquals("Pragmatist Numbers Sum", MULTIPLE_AGGS_PRAG_NUM_SUM, hit.get("Number Stats Sum"));
            }
            aggsCount++;
        }
        search.close();
        Assert.assertEquals("Multiple Aggregations Returns "+ MULTIPLE_NUM_AGGS +" Rows",MULTIPLE_NUM_AGGS, aggsCount);
    }

    private String setQuerySize(String query, int size)
    {
        JSONObject queryObject = new JSONObject(query);
        queryObject.put("size", size);
        return queryObject.toString();
    }

    private static int countIndexed()
    {
        int indexed = 0;

        HttpGet statusGet = new HttpGet("http://localhost:9500/unit/_stats");
        try
        {
            CloseableHttpClient httpclient = HttpClientBuilder.create().build();
            HttpResponse response = httpclient.execute(statusGet);
            InputStream isResponse = response.getEntity().getContent();
            BufferedReader isReader = new BufferedReader(new InputStreamReader(isResponse));
            StringBuilder strBuilder = new StringBuilder();
            String readLine;
            while ((readLine = isReader.readLine()) != null)
            {
                strBuilder.append(readLine);
            }
            isReader.close();
            JSONObject esResponse = new JSONObject(strBuilder.toString());
            indexed = esResponse.getJSONObject("indices").getJSONObject("unit").getJSONObject("total")
                    .getJSONObject("docs").getInt("count");
            httpclient.close();
        }
        catch (ClientProtocolException e1)
        {
            e1.printStackTrace();
        }
        catch (IOException e1)
        {
            e1.printStackTrace();
        }

        return indexed;
    }

    private static void populateTest()
    {
        // Get the bulk index as a stream
        InputStream bulkStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("bulk-insert.json");
        CloseableHttpClient httpclient = HttpClientBuilder.create().build();

        HttpResponse response;
        // Drop the index if it's there
        HttpDelete httpdelete = new HttpDelete("http://localhost:9500/unit*");
        try
        {
            response = httpclient.execute(httpdelete);
            logger.debug("Index Deleted: " + response);
            httpclient.close();
        }
        catch (ClientProtocolException protoException)
        {
            logger.warn("Protocol Error while deleting index: " + protoException);
        }
        catch (IOException ioException)
        {
            logger.warn("IO Error while deleting index: " + ioException);
        }

        HttpPost httppost = new HttpPost("http://localhost:9500/_bulk");

        InputStreamEntity isEntity = new InputStreamEntity(bulkStream);
        httppost.setEntity(isEntity);

        try
        {
            httpclient = HttpClientBuilder.create().build();
            response = httpclient.execute(httppost);
            logger.debug(response.getStatusLine());
            httpclient.close();
        }
        catch (ClientProtocolException protoException)
        {
            logger.warn("Protocol Error while bulk indexing: " + protoException);
        }
        catch (IOException ioException)
        {
            logger.warn("IO Error while bulk indexing: " + ioException);
        }
        logger.debug("Waiting for index to settle down...");
        while (countIndexed() < 25)
        {
        }
        logger.debug("...done!");
    }

    private static String getQuery(String queryName)
    {

        StringBuffer sb = new StringBuffer();

        BufferedReader br;

        try
        {
            br = new BufferedReader(
                    new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(queryName),
                            "UTF-8"));
            for (int c = br.read(); c != -1; c = br.read())
                sb.append((char) c);
        }
        catch (Exception e)
        {
            logger.error("Failed to read query: " + e);
        }

        String queryString = sb.toString();
        logger.debug("Loaded Query: " + queryString);
        return queryString;
    }

    private static Node startESNode(String clusterName)
    {

        Settings nodeSettings = Settings.settingsBuilder()
        		.put("path.home", "target")
        		.put("transport.tcp.port", "9600-9700")
                .put("http.port", "9500")
                .put("http.max_content_length", "100M").build();

        Node node = NodeBuilder.nodeBuilder()
        		.settings(nodeSettings)
        		.clusterName(clusterName).node();

        node.start();

        return node;
    }

    private static void stopNode(Node node)
    {
        node.close();
    }
}
