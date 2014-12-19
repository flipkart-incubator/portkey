/**
 * 
 */
package com.flipkart.portkey.common.entity.persistence;

import java.util.List;

import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.FailureAction;

/**
 * @author santosh.p
 */
public class EntityPersistenceWriteConfig
{
	private List<DataStoreType> writeOrder;
	private FailureAction failureAction;

	public List<DataStoreType> getWriteOrder()
	{
		return writeOrder;
	}

	public void setWriteOrder(List<DataStoreType> writeOrder)
	{
		this.writeOrder = writeOrder;
	}

	public FailureAction getFailureAction()
	{
		return failureAction;
	}

	public void setFailureAction(FailureAction failureAction)
	{
		this.failureAction = failureAction;
	}
}
