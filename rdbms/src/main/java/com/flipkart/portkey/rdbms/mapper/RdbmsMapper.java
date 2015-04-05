/**
 * 
 */
package com.flipkart.portkey.rdbms.mapper;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.jdbc.core.RowMapper;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.serializer.Serializer;
import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;

/**
 * @author santosh.p
 */
public class RdbmsMapper<V extends Entity> implements RowMapper<V>
{
	private static final Logger logger = Logger.getLogger(RdbmsMapper.class);
	private Class<V> clazz;
	private static Map<Class<? extends Entity>, RdbmsMapper<? extends Entity>> classToMapperMap =
	        new HashMap<Class<? extends Entity>, RdbmsMapper<? extends Entity>>();
	private static final ObjectMapper mapper = new ObjectMapper();

	protected RdbmsMapper(Class<V> clazz)
	{
		this.clazz = clazz;
	}

	public static <T extends Entity> RdbmsMapper<T> getInstance(Class<T> clazz)
	{
		if (classToMapperMap.containsKey(clazz))
		{
			return (RdbmsMapper<T>) classToMapperMap.get(clazz);
		}
		RdbmsMapper<T> mapper = new RdbmsMapper(clazz);
		classToMapperMap.put(clazz, mapper);
		return mapper;
	}

	private static boolean isTypeRdbmsCompatible(Class clazz)
	{
		if (clazz.isPrimitive() || clazz.equals(Boolean.class) || clazz.equals(Short.class)
		        || clazz.equals(Integer.class) || clazz.equals(Long.class) || clazz.equals(BigInteger.class)
		        || clazz.equals(Double.class) || clazz.equals(BigDecimal.class) || clazz.equals(String.class)
		        || clazz.equals(Date.class) || clazz.equals(java.sql.Date.class) || clazz.equals(Timestamp.class)
		        || clazz.equals(Time.class))
		{
			return true;
		}
		return false;
	}

	public static <T extends Entity> Object get(T bean, String fieldName)
	{
		try
		{
			PropertyDescriptor javaField = PropertyUtils.getPropertyDescriptor(bean, fieldName);
			Object value = javaField.getReadMethod().invoke(bean);
			return get(bean.getClass(), fieldName, value);
		}
		catch (IllegalAccessException e)
		{
			e.printStackTrace();
		}
		catch (InvocationTargetException e)
		{
			e.printStackTrace();
		}
		catch (NoSuchMethodException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public static <T extends Entity> Object get(Class<T> clazz, String fieldName, Object value)
	{

		if (value == null || isTypeRdbmsCompatible(value.getClass()))
		{
			return value;
		}
		else if (value.getClass().isEnum())
		{
			return PortKeyUtils.enumToString((Enum) value);
		}
		else if (value.getClass().getName().contains("JSON"))
		{
			return value.toString();
		}
		RdbmsMetaDataCache metaDataCache = RdbmsMetaDataCache.getInstance();
		RdbmsTableMetaData tableMetaData = metaDataCache.getMetaData(clazz);
		Serializer serializer = tableMetaData.getSerializerFromFieldName(fieldName);
		Object serialized = serializer.serialize(value);
		return serialized;
	}

	public static <T extends Entity> void put(T bean, String fieldName, Object value)
	{

		try
		{
			if (null != value && "" != value)
			{
				PropertyDescriptor javaField = PropertyUtils.getPropertyDescriptor(bean, fieldName);

				if (javaField == null)
				{
					return;
				}
				else if (isTypeRdbmsCompatible(javaField.getPropertyType()) || javaField.getPropertyType().isEnum())
				{
					javaField.getWriteMethod().invoke(bean, mapper.convertValue(value, javaField.getPropertyType()));
				}
				else
				{
					RdbmsMetaDataCache metaDataCache = RdbmsMetaDataCache.getInstance();
					RdbmsTableMetaData tableMetaData = metaDataCache.getMetaData(bean.getClass());
					Serializer serializer = tableMetaData.getSerializerFromFieldName(fieldName);
					Object deserialized = serializer.deserialize(value.toString(), javaField.getPropertyType());
					BeanUtils.setProperty(bean, fieldName, deserialized);
				}
			}
		}
		catch (IllegalAccessException e)
		{
			logger.debug(e);
			e.printStackTrace();
		}
		catch (InvocationTargetException e)
		{
			logger.debug(e);
			e.printStackTrace();
		}
		catch (NoSuchMethodException e)
		{
			logger.debug(e);
			e.printStackTrace();
		}
		catch (IllegalArgumentException e)
		{
			logger.debug(e);
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	public V mapRow(ResultSet resultSet, int rowNum) throws SQLException
	{
		// make sure resultset is not null
		if (resultSet == null)
		{
			return null;
		}
		V bean;
		try
		{
			bean = clazz.newInstance();
		}
		catch (InstantiationException e)
		{
			logger.warn("Exception in creating class instance:" + e);
			return null;
		}
		catch (IllegalAccessException e)
		{
			logger.warn("Exception in creating class instance:" + e);
			return null;
		}

		RdbmsTableMetaData tableMetaData;
		tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		ResultSetMetaData metadata = resultSet.getMetaData();

		for (int i = 1; i <= metadata.getColumnCount(); i++)
		{
			String columnName = metadata.getColumnLabel(i);
			String fieldName = tableMetaData.getColumnNameToFieldNameMap().get(columnName);

			if ((fieldName == null || fieldName.isEmpty()))
			{
				fieldName = columnName;
			}
			Object columnValue = resultSet.getObject(i);
			put(bean, fieldName, columnValue);
		}
		return bean;
	}
}
