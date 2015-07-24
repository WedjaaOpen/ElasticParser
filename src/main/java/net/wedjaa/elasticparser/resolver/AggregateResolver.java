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

package net.wedjaa.elasticparser.resolver;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;

public class AggregateResolver {

    private static final String AGGREGATION_NULL = "Null";
    public final static String AGGREGATION_BUCKETS = "Buckets";
    public final static String AGGREGATION_SIMPLE = "Simple";

    private ClassFinder classFinder;


	private static AggregateResolver instance = null;

	private static final Logger logger = Logger.getLogger(AggregateResolver.class);

	protected AggregateResolver() {
		this.classFinder = new ClassFinder();
	}

	public static AggregateResolver getInstance() {

		if (instance == null) {
			instance = new AggregateResolver();
		}

		return instance;

	}

	private Class<?> getAggregationClass(Aggregation aggregation) {
		return aggregation.getClass();
	}

	@SuppressWarnings("unchecked")
	private List<Class<?>> getBuckets(Aggregation aggregation) {

		List<Class<?>> buckets = null;

		Class<?> aggregationClass = getAggregationClass(aggregation);

		if (aggregationClass == null) {
			logger.warn("Can't get entries for unknown aggregation type: "
					+ aggregation.getClass());
			return null;
		}

		Method getBucketsMethod = classFinder.getMethod("getBuckets", aggregationClass);
		if (getBucketsMethod == null) {
			logger.warn("Seems that " + aggregationClass.getCanonicalName()
					+ " has no getBuckets method. Funny you should call it.");
			return null;
		}

		try {
			buckets = (List<Class<?>>) getBucketsMethod.invoke(aggregation);
			if (buckets.size() > 0) {
				logger.debug("Returning " + buckets.size() + " buckets.");
			}
		} catch (IllegalAccessException e) {
			logger.trace("Failed to get entries on aggregation of type " + aggregation.getName() + ": " + e
                    .getLocalizedMessage());
		} catch (IllegalArgumentException e) {
			logger.trace("Failed to get entries on aggregation of type " + aggregation.getName() + ": " + e
                    .getLocalizedMessage());
		} catch (InvocationTargetException e) {
			logger.trace("Failed to get entries on aggregation of type " + aggregation.getName() + ": " + e
                    .getLocalizedMessage());
		}

		return buckets;
	}



	private Map<String, Object> createBucketsMap(
			Aggregation aggregation, Object bucket,
			Class<?> bucketClass, String parentAggregation) {

		Map<String, Object> result = new HashMap<String, Object>();

        String aggregationName = aggregation.getName();

		logger.debug("createBucketsMap - entry[" + parentAggregation + "] = " + aggregationName);
		result.put(parentAggregation, aggregation.getName());

		List<Method> bucketMethods = classFinder.getClassMethods(bucketClass);

		for (Method method : bucketMethods) {
			if (method.getName().startsWith("get")) {
				String key = method.getName().substring(3);
				if (!key.equals("Class") && !key.equals("Aggregations")) {
					try {
                        // Make the method accessible by
                        // default.
                        method.setAccessible(true);
						Object value = method.invoke(bucket);
                        String effectiveKey =  aggregationName + " " + key;
						if (value != null && !value.toString().equals("NaN")) {
							logger.debug("   entry["  + effectiveKey + "] = " + value);
							result.put( effectiveKey , value);
						} else {
							// We need to fill in missing data in aggregations
							// or Jasper will choke on it at times.
							if ( method.getReturnType().getCanonicalName().equals(String.class.getCanonicalName()) ) {
								// Fill in the String
								result.put(effectiveKey, "");
							} else {
								result.put(effectiveKey, 0.0);
							}
						}
					} catch (IllegalAccessException e) {
						logger.trace("Failed to execute method " + method.getName() + " on entry: " + bucket + ": " + e
                                .toString());
					} catch (IllegalArgumentException e) {
						logger.trace("Failed to execute method " + method.getName() + " on entry: " + bucket + ": " + e
                                .toString());
					} catch (InvocationTargetException e) {
						logger.trace("Failed to execute method " + method.getName() + " on entry: " + bucket + ": " + e
                                .toString());
					}
				}
			}
		}

		return result;
	}

	public List<Map<String, Object>> unrollSimpleAggregation(Aggregation aggregation, String parentAggregation, int depth) {

		if ( parentAggregation == null ) {
			parentAggregation = "";
		} 

		logger.debug("unrollSimple: " + aggregation.getName() + "; Parent: " + parentAggregation +"; Depth: " + depth);
		Class<?> aggClass = aggregation.getClass();
		List<Map<String, Object>> subValues = null;
		if ( classFinder.hasMethod("getAggregations", aggClass) ) {
			logger.debug("This is a single bucket aggregation of some sort....");
			Method getAggregationsMethod = classFinder.getMethod("getAggregations", aggClass);
			if ( getAggregationsMethod != null ) {
				try {
                    getAggregationsMethod.setAccessible(true);
					Aggregations subAggregations = (Aggregations) getAggregationsMethod.invoke(aggregation);
					subValues = explode(subAggregations, aggregation.getName(), depth+1);
				} catch (IllegalArgumentException e) {
					logger.trace("Illegal argument exception calling getAggregations on " + aggClass);
				} catch (IllegalAccessException e) {
					logger.trace("Illegal access exception calling getAggregations on " + aggClass);
				} catch (InvocationTargetException e) {
					logger.trace("Illegal Invocation Target exception calling getAggregations on " + aggClass);
				}
			}
		}
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		Class<?> aggregationClass = getAggregationClass(aggregation);
		result.add(createBucketsMap(aggregation, aggregation, aggregationClass, parentAggregation));
		if ( subValues != null ) {
			logger.debug("Extending simple " + aggregation.getName() + " with contained aggregations.");
			Iterator<Map<String, Object>> subIter = subValues.iterator();
			while ( subIter.hasNext() ) {
				Map<String, Object> subMap = subIter.next();
				Iterator<Map<String,Object>> resIter = result.iterator();
				while ( resIter.hasNext() ) {
					Map<String,Object> resultMap = resIter.next();
					Iterator<String> resKeyIter = resultMap.keySet().iterator(); 
					while ( resKeyIter.hasNext() ) {
						String resKey = resKeyIter.next();
						// Don't overwrite inner values
						if ( !subMap.containsKey(resKey) ) {
							subMap.put(resKey, resultMap.get(resKey));
						}
					}
				}
			}
			result = subValues;
		}
		logger.debug("Simple Returning: " + result);
		return result;
	}

    /**
     * Adds the contents of a simple bucket to an entry. This method will alter the contents of the
     * entry that's passed to it.
     *
     * @param entryMap                  The entry to populate with the bucket data.
     * @param aggregation               The aggregation bucket containing the data.
     * @param parentAggregationName     The parent aggregation name.
     * @param depth                     The depth of this unrolling.
     *
     */
    private void addBucket(Map<String, Object> entryMap, Aggregation aggregation, String parentAggregationName, int depth) {
        List<Map<String, Object>> subEntry = unrollSimpleAggregation(aggregation, parentAggregationName, depth);
        Iterator<Map<String, Object>> subIter = subEntry.iterator();
        while (subIter.hasNext())
        {
            Map<String, Object> subMap = subIter.next();
            entryMap.putAll(subMap);
        }
    }

	public List<Map<String, Object>> unrollAggregationBuckets(Aggregation aggregation, String parentAggregation, int depth) {

		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

		if ( parentAggregation == null ) {
			parentAggregation = aggregation.getName();
		}

        logger.debug("unrollBucket: " + aggregation.getName() + "; Parent: " + parentAggregation +"; Depth: " + depth);

		List<Class<?>> buckets = getBuckets(aggregation);

		if (buckets != null) {


			for (Object bucket : buckets) {

                //  These are the buckets that need to be split into
                //  multiple entries.
                List<List<Map<String, Object>>> splitBuckets = new ArrayList<>();
                Map<String, Object> entryMap = createBucketsMap(aggregation, bucket, bucket.getClass(), parentAggregation);

				logger.debug("Bucket Class: " + bucket.getClass());

				Method getAggregationsMethod = classFinder.getMethod("getAggregations", bucket.getClass());
				if ( getAggregationsMethod != null )
                {
                    try
                    {
                        logger.debug("Processing " + aggregation.getName() + " buckets.");
                        Aggregations bucketAggregations = (Aggregations) getAggregationsMethod.invoke(bucket);
                        for (Aggregation bucketAggregation : bucketAggregations.asList())
                        {
                            logger.debug("Bucket " +  bucketAggregation.getName() +", " + bucketAggregation.getClass().getName());
                            if (getAggregationType(bucketAggregation).equals(AGGREGATION_SIMPLE)
                                    && !bucketAggregation.getClass().getName().endsWith("Nested") )
                            {
                                logger.debug("This bucket ["+ bucketAggregation.getName() +"] goes into entry: " + aggregation.getName());
                                addBucket(entryMap, bucketAggregation, aggregation.getName(), depth + 1);
                            }
                            else
                            {

                                logger.debug("This bucket [" + bucketAggregation.getName() + "] gets split into multiple entries.");
                                if ( bucketAggregation.getClass().getName().endsWith("Nested") )   {
                                    splitBuckets.add(unrollSimpleAggregation(bucketAggregation, aggregation.getName(), depth + 1));
                                } else
                                {
                                    splitBuckets.add(unrollAggregationBuckets(bucketAggregation, aggregation.getName(),
                                            depth + 1));
                                }
                            }
                        }
                        logger.debug(aggregation.getName() + " buckets exploration complete!");

                        if (splitBuckets.size() > 0)
                        {
                            logger.debug("Splitting into multiple entries");
                            Iterator<List<Map<String, Object>>> splitBucketsIter = splitBuckets.iterator();
                            while (splitBucketsIter.hasNext())
                            {
                                Iterator<Map<String, Object>> subEntryIter = splitBucketsIter.next().iterator();
                                while (subEntryIter.hasNext())
                                {
                                    Map<String, Object> subMap = subEntryIter.next();
                                    subMap.putAll(entryMap);
                                    logger.debug("Adding " + subMap + " to results for " + aggregation.getName());
                                    result.add(subMap);
                                }
                            }
                        }
                        else
                        {
                            logger.debug("["+aggregation.getName()+"] Returning a single entry: " + entryMap);
                            result.add(entryMap);
                        }
                    }
                    catch (IllegalArgumentException e)
                    {
                        logger.trace("Illegal argument exception calling getAggregations on " + bucket);
                    }
                    catch (IllegalAccessException e)
                    {
                        logger.trace("Illegal access exception calling getAggregations on " + bucket);
                    }
                    catch (InvocationTargetException e)
                    {
                        logger.trace("Illegal Invocation Target exception calling getAggregations on " + bucket);
                    }
                }

			}
		}
		logger.debug("Unrolled Returning: " + result);

		return result;
	}

    public String getAggregationType(Aggregation aggregation) {

        Class<?> aggregationClass = aggregation.getClass();

        if (aggregationClass == null) {
            return AGGREGATION_NULL;
        }

        if ( classFinder.getClassMethods(aggregationClass) == null ) {
            return AGGREGATION_NULL;
        }

        if (classFinder.hasMethod("getBuckets", aggregationClass)) {
            return AGGREGATION_BUCKETS;
        }

        return AGGREGATION_SIMPLE;
    }

	public List<Map<String, Object>> unrollAggregation(InternalAggregation aggregation, String parentAggregation, int depth) {

		String aggregationType = getAggregationType(aggregation);

        if ( aggregationType.equals(AGGREGATION_NULL) ) {
            logger.warn("Failed to unroll aggregation");
        }

        if ( aggregationType.equals(AGGREGATION_BUCKETS)) {
            logger.trace("Is a bucket type of aggregation, unrolling it");
            return unrollAggregationBuckets(aggregation, parentAggregation, depth);
        }

        logger.trace("Is a single aggregation - like statistics");
        return unrollSimpleAggregation(aggregation, parentAggregation, depth);
	}

	private List<Map<String, Object>> explode(Aggregations aggregations, String parentAggregation, int depth) {
		return explode(aggregations.asMap(), parentAggregation, depth);
	}

	public List<Map<String, Object>> explode(Aggregations aggregations) {
		return explode(aggregations.asMap(), "Aggregation", 0);
	}

	public List<Map<String, Object>> explode(Map<String, Aggregation> aggregations) {
		return  explode(aggregations, "Aggregation", 0);
	}
	
	private List<Map<String, Object>> explode(Map<String, Aggregation> aggregations, String parentAggregation, int depth) {

		List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>();

		logger.debug("exploder - Parent: " + parentAggregation);

		Set<String> aggregation_names = aggregations.keySet();
		for (String aggregation_name : aggregation_names) {
			entries.addAll(unrollAggregation((InternalAggregation) aggregations.get(aggregation_name), parentAggregation, depth));
		}

		return entries;
	}


}