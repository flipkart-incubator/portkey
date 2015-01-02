/**
 * 
 */
package com.flipkart.portkey.common.serializer;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author santosh.p
 */
public class JsonSerializer extends Serializer
{
	private static Logger logger = Logger.getLogger(JsonSerializer.class);

	/**
     * 
     */
	public JsonSerializer()
	{
		mapper = new ObjectMapper();
	}

	private final ObjectMapper mapper;

	@Override
	public String serialize(Object obj)
	{
		try
		{
			return mapper.writeValueAsString(obj);
		}
		catch (JsonGenerationException e)
		{
			logger.info("Exception while trying to serialize object, object=" + obj, e);
		}
		catch (JsonMappingException e)
		{
			logger.info("Exception while trying to serialize object, object=" + obj, e);
		}
		catch (IOException e)
		{
			logger.info("Exception while trying to serialize object, object=" + obj, e);
		}
		return null;
	}

	@Override
	public <T> T deserialize(String serialized, Class<T> clazz)
	{
		if (serialized == null)
		{
			return null;
		}
		try
		{
			return mapper.readValue(serialized, clazz);
		}
		catch (JsonParseException e)
		{
			logger.info("Exception while trying to deserialize string, string=" + serialized + " class=" + clazz, e);
		}
		catch (JsonMappingException e)
		{
			logger.info("Exception while trying to deserialize string, string=" + serialized + " class=" + clazz, e);
		}
		catch (IOException e)
		{
			logger.info("Exception while trying to deserialize string, string=" + serialized + " class=" + clazz, e);
		}
		return null;
	}
}
