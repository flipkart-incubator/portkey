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
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

import com.flipkart.portkey.common.entity.JoinEntity;
import com.flipkart.portkey.common.serializer.Serializer;
import com.flipkart.portkey.common.util.PortKeyUtils;
import com.flipkart.portkey.rdbms.metadata.RdbmsJoinMetaData;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;

/**
 * @author santosh.p
 */
public class RdbmsJoinMapper<V extends JoinEntity> implements RowMapper<V>
{
	private static final Logger logger = LoggerFactory.getLogger(RdbmsJoinMapper.class);
	private Class<V> clazz;
	private static Map<Class<? extends JoinEntity>, RdbmsJoinMapper<? extends JoinEntity>> classToMapperMap =
	        new HashMap<Class<? extends JoinEntity>, RdbmsJoinMapper<? extends JoinEntity>>();
	private static final ObjectMapper mapper = new ObjectMapper();

	protected RdbmsJoinMapper(Class<V> clazz)
	{
		this.clazz = clazz;
	}

	public static <T extends JoinEntity> RdbmsJoinMapper<T> getInstance(Class<T> clazz)
	{
		if (classToMapperMap.containsKey(clazz))
		{
			return (RdbmsJoinMapper<T>) classToMapperMap.get(clazz);
		}
		RdbmsJoinMapper<T> mapper = new RdbmsJoinMapper(clazz);
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

	public static <T extends JoinEntity> Object get(T bean, String fieldName)
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

	public static <T extends JoinEntity> Object getForJoin(Class<T> clazz, String fieldName, Object value)
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
		RdbmsJoinMetaData joinMetaData = metaDataCache.getJoinMetaData(clazz);
		Serializer serializer = joinMetaData.getSerializerFromFieldName(fieldName);
		Object serialized = serializer.serialize(value);
		return serialized;
	}

	public static <T extends JoinEntity> Object get(Class<T> clazz, String fieldName, Object value)
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
		RdbmsTableMetaData tableMetaData = metaDataCache.getTableMetaData(clazz);
		Serializer serializer = tableMetaData.getSerializerFromFieldName(fieldName);
		Object serialized = serializer.serialize(value);
		return serialized;
	}

	public static <T extends JoinEntity> void put(T bean, String fieldName, Object value)
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
					RdbmsTableMetaData tableMetaData = metaDataCache.getTableMetaData(bean.getClass());
					Serializer serializer = tableMetaData.getSerializerFromFieldName(fieldName);
					Object deserialized = serializer.deserialize(value.toString(), javaField.getPropertyType());
					BeanUtils.setProperty(bean, fieldName, deserialized);
				}
			}
		}
		catch (IllegalAccessException e)
		{
			logger.info("Exception while trying to set field value into bean, bean:" + bean + ", fieldName:"
			        + fieldName + ", value:" + value, e);
		}
		catch (InvocationTargetException e)
		{
			logger.info("Exception while trying to set field value into bean, bean:" + bean + ", fieldName:"
			        + fieldName + ", value:" + value, e);
		}
		catch (NoSuchMethodException e)
		{
			logger.info("Exception while trying to set field value into bean, bean:" + bean + ", fieldName:"
			        + fieldName + ", value:" + value, e);
		}
		catch (IllegalArgumentException e)
		{
			logger.info("Exception while trying to set field value into bean, bean:" + bean + ", fieldName:"
			        + fieldName + ", value:" + value, e);
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

		RdbmsJoinMetaData joinMetaData;
		joinMetaData = RdbmsMetaDataCache.getInstance().getJoinMetaData(clazz);
		ResultSetMetaData metadata = resultSet.getMetaData();

		for (int i = 1; i <= metadata.getColumnCount(); i++)
		{
			String columnName = metadata.getColumnLabel(i);
			String fieldName = joinMetaData.getColumnNameToFieldNameMap().get(columnName);

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
