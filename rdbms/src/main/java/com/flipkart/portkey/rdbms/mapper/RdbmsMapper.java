/**
 * 
 */
package com.flipkart.portkey.rdbms.mapper;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.jdbc.core.RowMapper;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.serializer.Serializer;
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

	public static <T extends Entity> Object get(T bean, String fieldName)
	{
		try
		{
			PropertyDescriptor javaField = PropertyUtils.getPropertyDescriptor(bean, fieldName);
			Object value = javaField.getReadMethod().invoke(bean);

			if (value != null && value.getClass().getName().contains("JSON"))
			{
				return value.toString();
			}
			return value;
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
				else if (javaField.getPropertyType().isEnum())
				{
					javaField.getWriteMethod().invoke(bean, mapper.convertValue(value, javaField.getPropertyType()));
				}
				else
				{

					RdbmsMetaDataCache metaDataCache = RdbmsMetaDataCache.getInstance();
					RdbmsTableMetaData tableMetaData = metaDataCache.getMetaData(bean.getClass());
					Serializer serializer = tableMetaData.getSerializer(fieldName);
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

		// try
		// {
		// if (value != null && value != "")
		// {
		// Field field = PortKeyUtils.getFieldFromBean(bean, fieldName);
		// if (field.getType().isPrimitive() || field.getType().equals(String.class))
		// {
		// field.set(bean, value);
		// }
		// else if (field.getType().isEnum())
		// {
		// field.set(bean, Enum.valueOf((Class<Enum>) field.getType(), value.toString()));
		// }
		// else if (field.getType().equals(Date.class))
		// {
		// Timestamp ts = (Timestamp) value;
		// Date date = ts;
		// field.set(bean, date);
		// }
		// else
		// {
		// RdbmsMetaDataCache metaDataCache = RdbmsMetaDataCache.getInstance();
		// RdbmsTableMetaData tableMetaData = metaDataCache.getMetaData(bean.getClass());
		// Serializer serializer = tableMetaData.getSerializer(fieldName);
		// Object deserialized = serializer.deserialize(value.toString(), field.getType());
		// BeanUtils.setProperty(bean, fieldName, deserialized);
		// }
		// }
		// }
		// catch (IllegalAccessException e)
		// {
		// logger.debug(e);
		// e.printStackTrace();
		// }
		// catch (InvocationTargetException e)
		// {
		// logger.debug(e);
		// e.printStackTrace();
		// }
		// catch (IllegalArgumentException e)
		// {
		// logger.debug(e);
		// e.printStackTrace();
		// }
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
			String fieldName = tableMetaData.getRdbmsColumnToFieldNameMap().get(columnName);

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
