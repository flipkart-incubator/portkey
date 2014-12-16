/**
 * 
 */
package com.flipkart.portkey.redis.keyparser;

import java.util.List;
import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.redis.metadata.RedisMetaData;

/**
 * @author santosh.p
 */
public interface KeyParserInterface
{
	public List<String> parsePrimaryKeyPattern(Entity bean, RedisMetaData metaData) throws PortKeyException;

	public List<String> parseSecondaryKeyPatterns(Entity bean, RedisMetaData metaData) throws PortKeyException;

	public List<String> parsePrimaryKeyPattern(Map<String, Object> attributeToValueMap, RedisMetaData metaData)
	        throws PortKeyException;

	public String parseSecondaryKeyPattern(Map<String, Object> attributeToValueMap, RedisMetaData metaData)
	        throws PortKeyException;
}
