/**
 * 
 */
package com.flipkart.portkey.common.serializer;

/**
 * @author santosh.p
 */
public abstract class Serializer
{
	public abstract String serialize(Object obj);

	public abstract <T> T deserialize(String serialized, Class<T> clazz);
}
