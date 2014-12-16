/**
 * 
 */
package com.flipkart.portkey.common.helper;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author santosh.p
 */
public class PortKeyHelper
{
	public static String toString(Object obj)
	{
		if (obj == null)
		{
			return null;
		}
		else
			return obj.toString();
	}

	/**
	 * @param value
	 * @return
	 */
	public static String toJsonString(Object objectToBeEncoded)
	{
		try
		{
			ObjectMapper mapper = new ObjectMapper();
			return mapper.writeValueAsString(objectToBeEncoded);
		}
		catch (Exception ignored)
		{
			return "";
		}
	}
}
