/**
 * 
 */
package com.flipkart.portkey.redis.mapper;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.flipkart.portkey.common.exception.BeanSerializationException;

/**
 * @author santosh.p
 */
public class DefaultRedisMapper implements RedisMapper
{
	private static Logger logger = Logger.getLogger(DefaultRedisMapper.class);
	private final ObjectMapper mapper;

	public DefaultRedisMapper()
	{
		mapper = new ObjectMapper();
	}

	public String serialize(Object obj)
	{
		try
		{
			return mapper.writeValueAsString(obj);
		}
		catch (JsonGenerationException e)
		{
			logger.info("Exception while trying to serialize " + obj + "\n" + e);
			throw new BeanSerializationException(e);
		}
		catch (JsonMappingException e)
		{
			logger.info("Exception while trying to serialize " + obj + "\n" + e);
			throw new BeanSerializationException(e);
		}
		catch (IOException e)
		{
			logger.info("Exception while trying to serialize " + obj + "\n" + e);
			throw new BeanSerializationException(e);
		}

	}

	public <T> T deserialize(String str, Class<T> clazz)
	{
		if (str == null)
		{
			return null;
		}
		try
		{
			return mapper.readValue(str, clazz);
		}
		catch (JsonParseException e)
		{
			logger.info("Exception while trying to deserialize String" + str + "\n" + e);
			throw new BeanSerializationException(e);
		}
		catch (JsonMappingException e)
		{
			logger.info("Exception while trying to deserialize String" + str + "\n" + e);
			throw new BeanSerializationException(e);
		}
		catch (IOException e)
		{
			logger.info("Exception while trying to deserialize String" + str + "\n" + e);
			throw new BeanSerializationException(e);
		}
	}
}
