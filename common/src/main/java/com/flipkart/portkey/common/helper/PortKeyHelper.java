/**
 * 
 */
package com.flipkart.portkey.common.helper;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * Helper class.
 * @author santosh.p
 */
public class PortKeyHelper
{
	/**
	 * @param obj
	 * @return string representation of object if obj is not null, null otherwise.
	 */
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
	 * @param objectToBeEncoded
	 * @return Json representation of passed object, an empty string in case of conversion failure.
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
