/**
 * 
 */
package com.flipkart.portkey.common.entity.persistence;

import java.util.List;

import com.flipkart.portkey.common.enumeration.DataStoreType;

/**
 * Configuration for reading an entity from data stores. It includes the list of data stores from which the entity can
 * be read and the order for the same.
 * @author santosh.p
 */
public class ReadConfig
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
