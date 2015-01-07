/**
 * 
 */
package com.flipkart.portkey.redis.mapper;

import com.flipkart.portkey.common.exception.BeanSerializationException;

/**
 * @author santosh.p
 */
public interface RedisMapper
{
	public String serialize(Object obj) throws BeanSerializationException;

	public <T> T deserialize(String str, Class<T> clazz) throws BeanSerializationException;
}
