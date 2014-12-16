/**
 * 
 */
package com.flipkart.portkey.redis.mapper;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

/**
 * @author santosh.p
 */
public interface RedisMapper
{
	public String serialize(Object obj) throws JsonGenerationException, JsonMappingException, IOException;

	public <T> T deserialize(String str, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException;
}
