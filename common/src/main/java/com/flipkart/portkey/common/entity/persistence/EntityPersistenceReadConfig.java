/**
 * 
 */
package com.flipkart.portkey.common.entity.persistence;

import java.util.List;

import com.flipkart.portkey.common.enumeration.DataStore;

/**
 * @author santosh.p
 */
public class EntityPersistenceReadConfig
{
	private List<DataStore> readOrder;

	public List<DataStore> getReadOrder()
	{
		return readOrder;
	}

	public void setReadOrder(List<DataStore> readOrder)
	{
		this.readOrder = readOrder;
	}
}
