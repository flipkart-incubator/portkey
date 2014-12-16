/**
 * 
 */
package com.flipkart.portkey.redis.mapper;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author santosh.p
 */
public class DefaultRedisMapper implements RedisMapper
{
	private final ObjectMapper mapper;

	public DefaultRedisMapper()
	{
		mapper = new ObjectMapper();
	}

	public String serialize(Object obj) throws JsonGenerationException, JsonMappingException, IOException
	{
		return mapper.writeValueAsString(obj);
	}

	public <T> T deserialize(String str, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException
	{
		if (str == null)
		{
			return null;
		}
		return mapper.readValue(str, clazz);
	}
}
