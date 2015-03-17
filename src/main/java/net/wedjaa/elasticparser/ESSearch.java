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

package net.wedjaa.elasticparser;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import net.wedjaa.elasticparser.pager.ESAggregationPager;
import net.wedjaa.elasticparser.pager.ESEmptyPager;
import net.wedjaa.elasticparser.pager.ESFacetsPager;
import net.wedjaa.elasticparser.pager.ESHitsPager;
import net.wedjaa.elasticparser.pager.ESResultsPager;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONObject;

public class ESSearch implements Connection
{

    private ESResultsPager pager;
    private boolean keepClient = false;
    private Client esClient;
    private String mainSearch;
    private String cluster;
    private String[] indexes;
    private String[] types;
    private String strIndexes;
    private String strTypes;
    private String username;
    private String password;
    private String hostname;
    private int port;
    private int searchMode;

    private SearchResponse searchResponse;
    private static Logger logger = Logger.getLogger(ESSearch.class);

    public final static int ES_MODE_HITS = 0;
    public final static int ES_MODE_FACETS = 1;
    public final static int ES_MODE_AGGS = 2;

    public final static String ES_DEFAULT_HOST = "localhost";
    public final static int ES_DEFAULT_PORT = 9300;
    public final static String ES_DEFAULT_CLUSTER = "elasticsearch";
    public final static int ES_DEFAULT_SEARCH_MODE = ES_MODE_HITS;

    /**
     * @return a ESSearch initialized to connect to the local node and search on
     * all the indexes and types returning the <em>Hits</em>.
     */
    public ESSearch()
    {
        this(null, null);
    }

    /**
     * @param indexes    a comma separated list of indexes that should be searched. If
     *                   <em>null</em> all the indexes will be searched.
     * @param types      a comma separated list of types that should be searched.If
     *                   <em>null</em> all the types will be searched.
     * @param searchMode the type of results we want to obtain: hits, facets or
     *                   aggregates
     * @return a ESSearch object initialized to connect to the local node with
     * the default connection parameters, returning what was specified
     * as a searchMode.
     */
    public ESSearch(String indexes, String types, int searchMode)
    {
        this(indexes, types, searchMode, ES_DEFAULT_HOST, ES_DEFAULT_PORT, null, null, ES_DEFAULT_CLUSTER);
    }

    /**
     * @param indexes a comma separated list of indexes that should be searched. If
     *                <em>null</em> all the indexes will be searched.
     * @param types   a comma separated list of types that should be searched.If
     *                <em>null</em> all the types will be searched.
     * @return a ESSearch object initialized to connect to the local node with
     * the default connection parameters, returning <em>Hits</em>.
     */
    public ESSearch(String indexes, String types)
    {
        this(indexes, types, ES_DEFAULT_SEARCH_MODE, ES_DEFAULT_HOST, ES_DEFAULT_PORT, null, null, ES_DEFAULT_CLUSTER);
    }

    /**
     * @param indexes  a comma separated list of indexes that should be searched. If
     *                 <em>null</em> all the indexes will be searched.
     * @param types    a comma separated list of types that should be searched.If
     *                 <em>null</em> all the types will be searched.
     * @param hostname hostname to connect to - or IP address
     * @param port     a port to connect to for trasport. The default transport port
     *                 is 9300.
     * @return a ESSearch object initialized to connect to the specified node to
     * search the specified indexes and types, returning <em>Hits</em>.
     */
    public ESSearch(String indexes, String types, String hostname, int port)
    {
        this(indexes, types, ES_DEFAULT_SEARCH_MODE, hostname, port, null, null, ES_DEFAULT_CLUSTER);
    }

    /**
     * @param indexes    a comma separated list of indexes that should be searched. If
     *                   <em>null</em> all the indexes will be searched.
     * @param types      a comma separated list of types that should be searched.If
     *                   <em>null</em> all the types will be searched.
     * @param searchMode the type of results we want to obtain: hits, facets or
     *                   aggregates
     * @param hostname   hostname to connect to - or IP address
     * @param port       a port to connect to for trasport. The default transport port
     *                   is 9300.
     * @return a ESSearch object initialized to connect to the specified node to
     * search the specified indexes and types, returning hits, facets or
     * aggregates depending on searchMode.
     */
    public ESSearch(String indexes, String types, int searchMode, String hostname, int port)
    {
        this(indexes, types, searchMode, hostname, port, null, null, ES_DEFAULT_CLUSTER);
    }

    /**
     * @param indexes  a comma separated list of indexes that should be searched. If
     *                 <em>null</em> all the indexes will be searched.
     * @param types    a comma separated list of types that should be searched.If
     *                 <em>null</em> all the types will be searched.
     * @param hostname hostname to connect to - or IP address
     * @param port     a port to connect to for transport. The default transport port
     *                 is 9300.
     * @param cluster  a cluster name to join. The default cluster name is
     *                 "elasticsearch".
     * @return a ESSearch object initialized to connect to the specified node
     * and cluster to search the specified indexes and types, returning
     * <em>Hits</em>.
     */
    public ESSearch(String indexes, String types, String hostname, int port, String cluster)
    {
        this(indexes, types, ES_DEFAULT_SEARCH_MODE, hostname, port, null, null, cluster);
    }

    /**
     * @param indexes    a comma separated list of indexes that should be searched. If
     *                   <em>null</em> all the indexes will be searched.
     * @param types      a comma separated list of types that should be searched.If
     *                   <em>null</em> all the types will be searched.
     * @param searchMode the type of results we want to obtain: hits, facets or
     *                   aggregates
     * @param hostname   hostname to connect to - or IP address
     * @param port       a port to connect to for transport. The default transport port
     *                   is 9300.
     * @param cluster    a cluster name to join. The default cluster name is
     *                   "elasticsearch".
     * @return a ESSearch object initialized to connect to the specified node
     * and cluster to search the specified indexes and types, returning
     * hits, facets or aggregates depending on searchMode.
     */
    public ESSearch(String indexes, String types, int searchMode, String hostname, int port, String cluster)
    {
        this(indexes, types, searchMode, hostname, port, null, null, cluster);
    }

    /**
     * @param indexes    a comma separated list of indexes that should be searched. If
     *                   <em>null</em> all the indexes will be searched.
     * @param types      a comma separated list of types that should be searched.If
     *                   <em>null</em> all the types will be searched.
     * @param searchMode the type of results we want to obtain: hits, facets or
     *                   aggregates
     * @param hostname   hostname to connect to - or IP address
     * @param port       a port to connect to for transport. The default transport port
     *                   is 9300.
     * @param username   the username to use to authenticate to ElasticSearch
     * @param password   the password of the user specified to connect to ElasticSearch
     * @param cluster    a cluster name to join. The default cluster name is
     *                   "elasticsearch".
     * @return a ESSearch object initialized to connect to the specified node
     * and cluster to search the specified indexes and types, returning
     * hits, facets or aggregates depending on searchMode. This
     * connection handles authentication with the username and password
     * provided.
     */
    public ESSearch(String indexes, String types, int searchMode, String hostname, int port, String username,
            String password, String cluster)
    {
        this.username = username;
        this.password = password;
        this.hostname = hostname;
        this.cluster = cluster;
        this.port = port;
        this.searchMode = searchMode;
        this.setIndexes(indexes);
        this.setTypes(types);
        this.strIndexes = indexes;
        this.strTypes = types;
        this.esClient = null;
    }

    /*
     * @param esClient An already connected ElasticSearch client that can be
     * used to execute the queries.
     *
     * @param searchMode The mode to use to interpret the results of the search
     * - hits, facets or aggregations
     *
     * @returns an instance of ESSearch that can be used to execute ES queries
     * and get the results specified in searchMode.
     */
    public ESSearch(Client esClient, int searchMode)
    {
        this.esClient = esClient;
        this.keepClient = true;
    }

    /*
     * Creates a clone of this ESSearch, keeping the searchMode
     *
     * @returns a clone of this ESSearch
     */
    public ESSearch clone()
    {
        if (esClient != null)
        {
            return new ESSearch(esClient, searchMode);
        }
        return new ESSearch(strIndexes, strTypes, searchMode, hostname, port, username, password, cluster);
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getHostname()
    {
        return hostname;
    }

    public void setHostname(String hostname)
    {
        this.hostname = hostname;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public void setSearch(String search)
    {
        logger.debug("Setting ElasticSearch query: " + search);
        mainSearch = search;
    }

    public String getSearch()
    {
        return mainSearch;
    }

    public void setIndexes(String indexes)
    {
        logger.debug("Setting search indexes: " + indexes);

        this.indexes = new String[0];

        if (indexes != null && indexes.length() > 0)
        {
            this.indexes = indexes.split("\\s*,\\s*");
        }

    }

    public void setIndexes(String[] indexes)
    {
        this.indexes = indexes;
    }

    public String[] getIndexes()
    {
        return this.indexes;
    }

    public void setTypes(String types)
    {

        logger.debug("Setting search types: " + types);

        this.types = new String[0];
        if (types != null && types.length() > 0)
        {
            this.types = types.split("\\s*,\\s*");
        }
    }

    public void setTypes(String[] types)
    {
        this.types = types;
    }

    public String[] getTypes()
    {
        return this.types;
    }

    public void connect()
    {

        if (esClient != null && keepClient)
        {
            logger.debug("We are already connected with a passed on client - not creating a new one");
            return;
        }

        logger.debug("Creating new client to connect to: " + this.hostname);

        // Prepare a client for the ES Server
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", this.cluster)
                .put("client.transport.sniff", true).build();

        InetSocketTransportAddress transportAddress = new InetSocketTransportAddress(this.hostname, this.port);
        TransportClient transportClient = new TransportClient(settings);
        transportClient.addTransportAddress(transportAddress);
        this.esClient = transportClient;

    }

    private SearchResponse getQueryCount(String query)
    {

        SearchRequestBuilder searchBuilder;

        if (indexes.length > 0)
        {
            searchBuilder = esClient.prepareSearch(indexes);
        }
        else
        {
            searchBuilder = esClient.prepareSearch();

        }
        if (types.length > 0)
        {
            searchBuilder.setTypes(types);
        }

        searchBuilder.setSearchType(SearchType.COUNT);

        SearchResponse searchRes = searchBuilder.setSource(query.getBytes()).execute().actionGet();

        return searchRes;

    }

    ;

    private SearchResponse executeSearch(String query, long start, int page_size)
    {

        SearchRequestBuilder searchBuilder;

        if (indexes.length > 0)
        {
            searchBuilder = esClient.prepareSearch(indexes);
        }
        else
        {
            searchBuilder = esClient.prepareSearch();

        }
        if (types.length > 0)
        {
            searchBuilder.setTypes(types);
        }

        if (start >= 0 && page_size >= 0)
        {
            JSONObject queryObject = new JSONObject(query);
            queryObject.put("size", page_size);
            queryObject.put("from", start);
            query = queryObject.toString();
        }

        SearchResponse searchRes = searchBuilder.setSource(query.getBytes()).execute().actionGet();

        return searchRes;

    }

    private SearchResponse executeSearch(String query)
    {
        return executeSearch(query, -1, -1);
    }

    private void runQuery(String query, boolean countOnly)
    {

        logger.debug("Complete search request: " + query);

        connect();

        SearchResponse searchRes;

        switch (searchMode)
        {
            case ESSearch.ES_MODE_HITS:
                if (countOnly)
                {
                    searchRes = getQueryCount(query);
                }
                else
                {
                    searchRes = executeSearch(query);
                }
                logger.debug("The query returns " + searchRes.getHits().getTotalHits() + " total matches.");
                logger.debug("Response: " + searchRes.toString());
                JSONObject queryObject = new JSONObject(query);
                long maxHits = searchRes.getHits().getTotalHits();
                if (queryObject.has("size"))
                {
                    maxHits = queryObject.getLong("size");
                    logger.debug("Limiting query hits to stated size: " + maxHits);
                }
                pager = new ESHitsPager(searchRes, query, maxHits);
                break;
            case ESSearch.ES_MODE_FACETS:
                // Facets will return all the results in one
                // query
                searchRes = executeSearch(query);
                pager = new ESFacetsPager(searchRes, query);
                break;
            case ESSearch.ES_MODE_AGGS:
                // Aggregations will return all the results in one
                // query
                searchRes = executeSearch(query);
                pager = new ESAggregationPager(searchRes, query);
                break;
            default:
                pager = new ESEmptyPager();
        }

        logger.trace("OK, ready to process the results");

    }

    private void runQuery(String query)
    {
        runQuery(query, true);
    }

    public void search(String query)
    {
        runQuery(query);
    }

    public void search()
    {
        search(mainSearch);
    }

    public void close()
    {
        if (this.esClient != null && !keepClient)
        {
            logger.debug("Disconnecting client");
            this.esClient.close();
            this.esClient = null;
        }
        if (this.esClient != null && keepClient)
        {
            logger.debug("Keeping client - it was not mine in the first place!");
        }
    }

    public Map<String, Object> next()
    {

        logger.debug("Next!");

        if (pager.done())
        {
            logger.debug("Pager is done - disposing of client.");
            if (this.esClient != null)
            {
                this.esClient.close();
            }
            return null;
        }

        if (!pager.hit_available())
        {
            logger.debug("Getting a page of hits - from: " + pager.current_hit_idx() + ", size: " + pager.page_size());
            // No hits are available - fetch the next
            // page of hits for the query
            searchResponse = executeSearch(pager.get_query(), pager.current_hit_idx(), pager.page_size());
            pager.set_page_size(searchResponse.getHits().getHits().length);
            logger.debug("Page hits: " + pager.page_size());
        }

        logger.debug("Returning hit at: " + (pager.current_hit_idx() + 1) + " of " + pager.page_size());

        return pager.next(searchResponse);

    }

    public Map<String, Class<?>> getFields(String query)
    {

        Map<String, Class<?>> result = new HashMap<String, Class<?>>();
        logger.debug("Getting fields using " + query);

        JSONObject queryObject = new JSONObject(query);
        queryObject.put("size", 1);
        String sizedQuery = queryObject.toString();
        // Let runQuery prepare the pager
        runQuery(sizedQuery, false);
        result = pager.getResponseFields();
        close();

        logger.debug("Fields: " + result.toString());
        return result;
    }

    public Map<String, Class<?>> getFields()
    {
        return getFields(mainSearch);
    }

    public void test()
    {
        String search = getSearch();
        setSearch("{ \"query\": { \"match_all\": {} }, \"size\": 0 }");
        getFields();
        setSearch(search);
    }

    @Override public boolean isWrapperFor(Class<?> wrappedClass) throws SQLException
    {
        return wrappedClass.equals(ESSearch.class);
    }

    @SuppressWarnings("unchecked") @Override public <T> T unwrap(Class<T> wrappedClass) throws SQLException
    {
        return (T) wrappedClass;
    }

    @Override public void clearWarnings() throws SQLException
    {
    }

    @Override public void commit() throws SQLException
    {
    }

    @Override public Array createArrayOf(String arg0, Object[] arg1) throws SQLException
    {
        return null;
    }

    @Override public Blob createBlob() throws SQLException
    {
        return null;
    }

    @Override public Clob createClob() throws SQLException
    {
        return null;
    }

    @Override public NClob createNClob() throws SQLException
    {
        return null;
    }

    @Override public SQLXML createSQLXML() throws SQLException
    {
        return null;
    }

    @Override public Statement createStatement() throws SQLException
    {
        return null;
    }

    @Override public Statement createStatement(int arg0, int arg1) throws SQLException
    {
        return null;
    }

    @Override public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException
    {
        return null;
    }

    @Override public Struct createStruct(String arg0, Object[] arg1) throws SQLException
    {
        return null;
    }

    /**
     * Sets the given schema name to access.
     * <p/>
     * If the driver does not support schemas, it will
     * silently ignore this request.
     * <p/>
     * Calling {@code setSchema} has no effect on previously created or prepared
     * {@code Statement} objects. It is implementation defined whether a DBMS
     * prepare operation takes place immediately when the {@code Connection}
     * method {@code prepareStatement} or {@code prepareCall} is invoked.
     * For maximum portability, {@code setSchema} should be called before a
     * {@code Statement} is created or prepared.
     *
     * @param schema the name of a schema  in which to work
     * @throws java.sql.SQLException if a database access error occurs
     *                               or this method is called on a closed connection
     * @see #getSchema
     * @since 1.7
     */
    @Override public void setSchema(String schema) throws SQLException
    {

    }

    /**
     * Retrieves this <code>Connection</code> object's current schema name.
     *
     * @return the current schema name or <code>null</code> if there is none
     * @throws java.sql.SQLException if a database access error occurs
     *                               or this method is called on a closed connection
     * @see #setSchema
     * @since 1.7
     */
    @Override public String getSchema() throws SQLException
    {
        return null;
    }

    /**
     * Terminates an open connection.  Calling <code>abort</code> results in:
     * <ul>
     * <li>The connection marked as closed
     * <li>Closes any physical connection to the database
     * <li>Releases resources used by the connection
     * <li>Insures that any thread that is currently accessing the connection
     * will either progress to completion or throw an <code>SQLException</code>.
     * </ul>
     * <p/>
     * Calling <code>abort</code> marks the connection closed and releases any
     * resources. Calling <code>abort</code> on a closed connection is a
     * no-op.
     * <p/>
     * It is possible that the aborting and releasing of the resources that are
     * held by the connection can take an extended period of time.  When the
     * <code>abort</code> method returns, the connection will have been marked as
     * closed and the <code>Executor</code> that was passed as a parameter to abort
     * may still be executing tasks to release resources.
     * <p/>
     * This method checks to see that there is an <code>SQLPermission</code>
     * object before allowing the method to proceed.  If a
     * <code>SecurityManager</code> exists and its
     * <code>checkPermission</code> method denies calling <code>abort</code>,
     * this method throws a
     * <code>java.lang.SecurityException</code>.
     *
     * @param executor The <code>Executor</code>  implementation which will
     *                 be used by <code>abort</code>.
     * @throws java.sql.SQLException if a database access error occurs or
     *                               the {@code executor} is {@code null},
     * @throws SecurityException     if a security manager exists and its
     *                               <code>checkPermission</code> method denies calling <code>abort</code>
     * @see SecurityManager#checkPermission
     * @see java.util.concurrent.Executor
     * @since 1.7
     */
    @Override public void abort(Executor executor) throws SQLException
    {

    }

    /**
     * Sets the maximum period a <code>Connection</code> or
     * objects created from the <code>Connection</code>
     * will wait for the database to reply to any one request. If any
     * request remains unanswered, the waiting method will
     * return with a <code>SQLException</code>, and the <code>Connection</code>
     * or objects created from the <code>Connection</code>  will be marked as
     * closed. Any subsequent use of
     * the objects, with the exception of the <code>close</code>,
     * <code>isClosed</code> or <code>Connection.isValid</code>
     * methods, will result in  a <code>SQLException</code>.
     * <p/>
     * <b>Note</b>: This method is intended to address a rare but serious
     * condition where network partitions can cause threads issuing JDBC calls
     * to hang uninterruptedly in socket reads, until the OS TCP-TIMEOUT
     * (typically 10 minutes). This method is related to the
     * {@link #abort abort() } method which provides an administrator
     * thread a means to free any such threads in cases where the
     * JDBC connection is accessible to the administrator thread.
     * The <code>setNetworkTimeout</code> method will cover cases where
     * there is no administrator thread, or it has no access to the
     * connection. This method is severe in it's effects, and should be
     * given a high enough value so it is never triggered before any more
     * normal timeouts, such as transaction timeouts.
     * <p/>
     * JDBC driver implementations  may also choose to support the
     * {@code setNetworkTimeout} method to impose a limit on database
     * response time, in environments where no network is present.
     * <p/>
     * Drivers may internally implement some or all of their API calls with
     * multiple internal driver-database transmissions, and it is left to the
     * driver implementation to determine whether the limit will be
     * applied always to the response to the API call, or to any
     * single  request made during the API call.
     * <p/>
     * <p/>
     * This method can be invoked more than once, such as to set a limit for an
     * area of JDBC code, and to reset to the default on exit from this area.
     * Invocation of this method has no impact on already outstanding
     * requests.
     * <p/>
     * The {@code Statement.setQueryTimeout()} timeout value is independent of the
     * timeout value specified in {@code setNetworkTimeout}. If the query timeout
     * expires  before the network timeout then the
     * statement execution will be canceled. If the network is still
     * active the result will be that both the statement and connection
     * are still usable. However if the network timeout expires before
     * the query timeout or if the statement timeout fails due to network
     * problems, the connection will be marked as closed, any resources held by
     * the connection will be released and both the connection and
     * statement will be unusable.
     * <p/>
     * When the driver determines that the {@code setNetworkTimeout} timeout
     * value has expired, the JDBC driver marks the connection
     * closed and releases any resources held by the connection.
     * <p/>
     * <p/>
     * This method checks to see that there is an <code>SQLPermission</code>
     * object before allowing the method to proceed.  If a
     * <code>SecurityManager</code> exists and its
     * <code>checkPermission</code> method denies calling
     * <code>setNetworkTimeout</code>, this method throws a
     * <code>java.lang.SecurityException</code>.
     *
     * @param executor     The <code>Executor</code>  implementation which will
     *                     be used by <code>setNetworkTimeout</code>.
     * @param milliseconds The time in milliseconds to wait for the database
     *                     operation
     *                     to complete.  If the JDBC driver does not support milliseconds, the
     *                     JDBC driver will round the value up to the nearest second.  If the
     *                     timeout period expires before the operation
     *                     completes, a SQLException will be thrown.
     *                     A value of 0 indicates that there is not timeout for database operations.
     * @throws java.sql.SQLException           if a database access error occurs, this
     *                                         method is called on a closed connection,
     *                                         the {@code executor} is {@code null},
     *                                         or the value specified for <code>seconds</code> is less than 0.
     * @throws SecurityException               if a security manager exists and its
     *                                         <code>checkPermission</code> method denies calling
     *                                         <code>setNetworkTimeout</code>.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @see SecurityManager#checkPermission
     * @see java.sql.Statement#setQueryTimeout
     * @see #getNetworkTimeout
     * @see #abort
     * @see java.util.concurrent.Executor
     * @since 1.7
     */
    @Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
    {

    }

    /**
     * Retrieves the number of milliseconds the driver will
     * wait for a database request to complete.
     * If the limit is exceeded, a
     * <code>SQLException</code> is thrown.
     *
     * @return the current timeout limit in milliseconds; zero means there is
     * no limit
     * @throws java.sql.SQLException           if a database access error occurs or
     *                                         this method is called on a closed <code>Connection</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @see #setNetworkTimeout
     * @since 1.7
     */
    @Override public int getNetworkTimeout() throws SQLException
    {
        return 0;
    }

    @Override public boolean getAutoCommit() throws SQLException
    {
        return false;
    }

    @Override public String getCatalog() throws SQLException
    {
        return null;
    }

    @Override public Properties getClientInfo() throws SQLException
    {
        return null;
    }

    @Override public String getClientInfo(String arg0) throws SQLException
    {
        return null;
    }

    @Override public int getHoldability() throws SQLException
    {
        return 0;
    }

    @Override public DatabaseMetaData getMetaData() throws SQLException
    {
        return null;
    }

    @Override public int getTransactionIsolation() throws SQLException
    {
        return 0;
    }

    @Override public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        return null;
    }

    @Override public SQLWarning getWarnings() throws SQLException
    {
        return null;
    }

    @Override public boolean isClosed() throws SQLException
    {
        return false;
    }

    @Override public boolean isReadOnly() throws SQLException
    {
        return false;
    }

    @Override public boolean isValid(int arg0) throws SQLException
    {
        return false;
    }

    @Override public String nativeSQL(String arg0) throws SQLException
    {
        return null;
    }

    @Override public CallableStatement prepareCall(String arg0) throws SQLException
    {
        return null;
    }

    @Override public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException
    {
        return null;
    }

    @Override public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException
    {
        return null;
    }

    @Override public PreparedStatement prepareStatement(String arg0) throws SQLException
    {
        return null;
    }

    @Override public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException
    {
        return null;
    }

    @Override public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException
    {
        return null;
    }

    @Override public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException
    {
        return null;
    }

    @Override public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException
    {
        return null;
    }

    @Override public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException
    {
        return null;
    }

    @Override public void releaseSavepoint(Savepoint arg0) throws SQLException
    {

    }

    @Override public void rollback() throws SQLException
    {

    }

    @Override public void rollback(Savepoint arg0) throws SQLException
    {

    }

    @Override public void setAutoCommit(boolean arg0) throws SQLException
    {

    }

    @Override public void setCatalog(String arg0) throws SQLException
    {

    }

    @Override public void setClientInfo(Properties arg0) throws SQLClientInfoException
    {

    }

    @Override public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException
    {

    }

    @Override public void setHoldability(int arg0) throws SQLException
    {

    }

    @Override public void setReadOnly(boolean arg0) throws SQLException
    {

    }

    @Override public Savepoint setSavepoint() throws SQLException
    {
        return null;
    }

    @Override public Savepoint setSavepoint(String arg0) throws SQLException
    {
        return null;
    }

    @Override public void setTransactionIsolation(int arg0) throws SQLException
    {

    }

    @Override public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException
    {

    }

}