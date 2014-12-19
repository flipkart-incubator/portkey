/**
 * 
 */
package com.flipkart.portkey.common.metadata;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.exception.PortKeyException;

/**
 * @author santosh.p
 */
public interface MetaDataCache
{
	public <T extends Entity> String getShardKey(Class<T> clazz) throws PortKeyException;
}
