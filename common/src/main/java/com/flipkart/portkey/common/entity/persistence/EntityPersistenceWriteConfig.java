/**
 * 
 */
package com.flipkart.portkey.common.entity.persistence;

import java.util.List;

import com.flipkart.portkey.common.enumeration.DataStore;
import com.flipkart.portkey.common.enumeration.FailureAction;

/**
 * @author santosh.p
 */
public class EntityPersistenceWriteConfig
{
	private List<DataStore> writeOrder;
	private FailureAction failureAction;

	public List<DataStore> getWriteOrder()
	{
		return writeOrder;
	}

	public void setWriteOrder(List<DataStore> writeOrder)
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
