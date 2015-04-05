/**
 * 
 */
package com.flipkart.portkey.common.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;

/**
 * @author santosh.p
 */
public class PortKeyUtils
{
	public static <T extends Entity> Field getFieldFromBean(T bean, String fieldName) throws InvalidAnnotationException
	{
		Field field = null;
		try
		{

			field = bean.getClass().getDeclaredField(fieldName);
			field.setAccessible(true);
		}
		catch (NoSuchFieldException e)
		{
			throw new InvalidAnnotationException("exception while trying to get field from field name, bean=" + bean
			        + ", fieldName=" + fieldName, e);
		}
		catch (SecurityException e)
		{
			throw new InvalidAnnotationException("exception while trying to get field from field name, bean=" + bean
			        + ", fieldName=" + fieldName, e);
		}
		return field;
	}

	public static <T extends Entity> Object getFieldValueFromBean(T bean, String fieldName)
	        throws InvalidAnnotationException
	{
		Object value = null;
		Field field = null;
		field = getFieldFromBean(bean, fieldName);
		try
		{
			field.setAccessible(true);
			value = field.get(bean);
		}
		catch (IllegalArgumentException e)
		{
			throw new InvalidAnnotationException("exception while trying to get value from bean and field, bean="
			        + bean + ", field=" + field, e);
		}
		catch (IllegalAccessException e)
		{
			throw new InvalidAnnotationException("exception while trying to get value from bean and field, bean="
			        + bean + ", field=" + field, e);
		}
		return value;
	}

	public static <T extends Entity> void setFieldValueInBean(T bean, String fieldName, Object value)
	        throws InvalidAnnotationException
	{
		Field field = null;
		field = getFieldFromBean(bean, fieldName);
		try
		{
			field.setAccessible(true);
			field.set(bean, value);
		}
		catch (IllegalArgumentException e)
		{
			throw new InvalidAnnotationException("exception while trying to set value of bean,, bean=" + bean
			        + ", field=" + field, e);
		}
		catch (IllegalAccessException e)
		{
			throw new InvalidAnnotationException("exception while trying to set value of bean, bean=" + bean
			        + ", field=" + field, e);
		}
	}

	/**
	 * @param obj
	 * @return string representation of object if obj is not null, null otherwise.
	 */
	public static String toString(Object obj)
	{
		return obj == null ? null : obj.toString();
	}

	public static <T1, T2> Map<T1, T2> mergeMaps(Map<T1, T2> map1, Map<T1, T2> map2)
	{
		Map<T1, T2> result = new HashMap<T1, T2>();
		result.putAll(map1);
		result.putAll(map2);
		return result;
	}

	public static String enumToString(Enum<?> e)
	{
		return e == null ? null : e.name();
	}
}
