package net.wedjaa.elasticparser.resolver;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class ClassFinder {

	private final static Logger logger = Logger.getLogger(ClassFinder.class);
	private Map<Class<?>, List<Method>> classMethods;
	
	public ClassFinder() {
		this.classMethods = new HashMap<Class<?>, List<Method>>();
	}
	
	public boolean hasMethod(String method, Class<?> objClass) {

		boolean result = false;

		List<Method> methods = getClassMethods(objClass);
		
		if (methods == null) {
			logger.warn("Failed to verify presence of " + method + " on class "
					+ objClass.getCanonicalName());
			return false;
		}

		Iterator<Method> iter = methods.iterator();
		while (iter.hasNext() && !result)
			result = iter.next().getName().equals(method);

		return result;

	}
	
	public Method getMethod(String method, Class<?> objClass) {

		Method result = null;
		
		List<Method> methods = getClassMethods(objClass);
		
		if (methods == null) {
			logger.warn("Failed to find method " + methods + " on class "
					+ objClass);
			return null;
		}

		Iterator<Method> iter = methods.iterator();
		while (iter.hasNext() && result == null) {
			Method current = iter.next();
			if (current.getName().equals(method)) {
				result = current;
			}
		}
		return result;
	}
	
	
	public List<Method> getClassMethods(Class<?> objClass) {

		if (!classMethods.containsKey(objClass)) {
			populateMethods(objClass);
		}

		return classMethods.get(objClass);
	}
	
	
	public static Set<Class<?>> getRelatedClasses(Class<?> clazz) {
	    List<Class<?>> res = new ArrayList<Class<?>>();
	    do {
	        res.add(clazz);
	        // First, add all the interfaces implemented by this class
	        Class<?>[] interfaces = clazz.getInterfaces();
	        if (interfaces.length > 0) {
	            res.addAll(Arrays.asList(interfaces));

	            for (Class<?> interfaze : interfaces) {
	                res.addAll(getRelatedClasses(interfaze));
	            }
	        }

	        // Add the super class
	        Class<?> superClass = clazz.getSuperclass();

	        // Interfaces does not have java,lang.Object as superclass, they have null, so break the cycle and return
	        if (superClass == null) {
	            break;
	        }

	        // Now inspect the superclass 
	        clazz = superClass;
	    } while (!"java.lang.Object".equals(clazz.getCanonicalName()));

	    return new HashSet<Class<?>>(res);
	}  
		

	private void populateMethods(Class<?> inspectedClass) {
		logger.trace("Examining methods for class " + inspectedClass);
		Method[] inspectedClassMethods = inspectedClass.getMethods();
		if (logger.isDebugEnabled()) {
			for (Method method : inspectedClassMethods) {
				logger.trace("  ->" + method + "  -> "
						+ method.getGenericReturnType());
			}
		}
		classMethods.put(inspectedClass, Arrays.asList(inspectedClassMethods));
	}

	public Class<?> getMethodReturnClass(Method method) {

		if (method == null) {
			logger.warn("Can't get return class type because of null method.");
			return null;
		}

		ParameterizedType genericClassType = (ParameterizedType) method
				.getGenericReturnType();

		Class<?> genericClass =  genericClassType.getClass();

		if ( genericClassType instanceof WildcardType) {
			WildcardType collectionClass = (WildcardType) genericClassType
					.getActualTypeArguments()[0];
			genericClass = (Class<?>) collectionClass.getUpperBounds()[0];
		} else {
			if ( genericClassType instanceof ParameterizedType ) {
				ParameterizedType parameterizedType = (ParameterizedType) genericClassType;
				if ( Class.class.isAssignableFrom( parameterizedType.getActualTypeArguments()[0].getClass() )) {
					genericClass = (Class<?>) parameterizedType.getActualTypeArguments()[0];
				} else {
					TypeVariable<?> returnType = (TypeVariable<?>) parameterizedType.getActualTypeArguments()[0];
					genericClass = (Class<?>) returnType.getBounds()[0];
				}
			}
		}
		
		return genericClass;
	}

}
