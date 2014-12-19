/**
 * 
 */
package com.flipkart.portkey.common.entity.persistence;

import java.util.List;

import com.flipkart.portkey.common.enumeration.DataStoreType;

/**
 * @author santosh.p
 */
public class EntityPersistenceReadConfig
{
	private List<DataStoreType> readOrder;

	public List<DataStoreType> getReadOrder()
	{
		return readOrder;
	}

	public void setReadOrder(List<DataStoreType> readOrder)
	{
		this.readOrder = readOrder;
	}
}
