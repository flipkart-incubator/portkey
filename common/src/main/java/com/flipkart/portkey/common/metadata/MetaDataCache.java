/**
 * 
 */
package com.flipkart.portkey.common.metadata;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.InvalidAnnotationException;

/**
 * @author santosh.p
 */
public interface MetaDataCache
{
	public <T extends Entity> String getShardKeyFieldName(Class<T> clazz) throws InvalidAnnotationException;
}
