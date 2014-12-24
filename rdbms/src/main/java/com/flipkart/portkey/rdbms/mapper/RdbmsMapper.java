/**
 * 
 */
package com.flipkart.portkey.rdbms.mapper;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.RowMapper;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;
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

	public RdbmsMapper(Class<V> clazz)
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
		try
		{
			tableMetaData = RdbmsMetaDataCache.getInstance().getMetaData(clazz);
		}
		catch (InvalidAnnotationException e)
		{
			// TODO: review this
			logger.info("Exception while trying to fetch metadata for class:" + clazz, e);
			return null;
		}
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
			Field field = tableMetaData.getFieldNameToFieldMap().get(fieldName);
			try
			{
				field.set(bean, columnValue);
			}
			catch (IllegalArgumentException e)
			{
				logger.info(e);
			}
			catch (IllegalAccessException e)
			{
				logger.info(e);
			}
		}
		return bean;
	}
}
