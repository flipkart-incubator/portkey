/**
 * 
 */
package com.flipkart.portkey.rdbms.mapper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
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

	public static <T extends Entity> Object get(T bean, String fieldName)
	{
		try
		{
			Field field = PortKeyUtils.getFieldFromBean(bean, fieldName);
			Object value = PortKeyUtils.getFieldValueFromBean(bean, fieldName);
			if (value == null || field.getType().isPrimitive() || field.getType().equals(String.class))
			{
				return value;
			}
			// TODO: return value instead of toString
			else if (field.getType().isEnum())
			{
				return value.toString();
			}
			else if (field.getType().equals(Date.class))
			{
				return value;
			}
			else if (field.getType().equals(Timestamp.class))
			{
				Timestamp ts = (Timestamp) value;
				Date date = new Date(ts.getTime());
				return date;
			}
			return mapper.writeValueAsString(value);
		}
		catch (JsonGenerationException e)
		{
			logger.info("Exception while fetching field value from bean, bean=" + bean + ", fieldName=" + fieldName, e);
		}
		catch (JsonMappingException e)
		{
			logger.info("Exception while fetching field value from bean, bean=" + bean + ", fieldName=" + fieldName, e);
		}
		catch (IOException e)
		{
			logger.info("Exception while fetching field value from bean, bean=" + bean + ", fieldName=" + fieldName, e);
		}
		catch (SecurityException e)
		{
			logger.info("Exception while fetching field value from bean, bean=" + bean + ", fieldName=" + fieldName, e);
		}
		return null;
	}

	public static <T extends Entity> void put(T bean, String fieldName, Object value)
	{
		try
		{
			if (value != null && value != "")
			{
				Field field = PortKeyUtils.getFieldFromBean(bean, fieldName);
				if (field.getType().isPrimitive() || field.getType().equals(String.class))
				{
					field.set(bean, value);
				}
				else if (field.getType().isEnum())
				{
					field.set(bean, Enum.valueOf((Class<Enum>) field.getType(), value.toString()));
				}
				else if (field.getType().equals(Date.class))
				{
					Timestamp ts = (Timestamp) value;
					Date date = ts;
					field.set(bean, date);
				}
				else
				{
					RdbmsMetaDataCache metaDataCache = RdbmsMetaDataCache.getInstance();
					RdbmsTableMetaData tableMetaData = metaDataCache.getMetaData(bean.getClass());
					Serializer serializer = tableMetaData.getSerializer(fieldName);
					Object deserialized = serializer.deserialize(value.toString(), field.getType());
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
			logger.info("Exception in creating class instance:" + e);
			return null;
		}
		catch (IllegalAccessException e)
		{
			logger.info("Exception in creating class instance:" + e);
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
