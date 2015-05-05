/**
 * 
 */
package com.flipkart.portkey.common.entity.persistence;

import java.util.List;

import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.FailureAction;

/**
 * Configuration for writing an entity into data stores. It includes the list of data stores to which the entity to be
 * written, the order for writes and the action to be executed(whether continue writing into other data stores or abort)
 * on write failure.
 * @author santosh.p
 */
public class WriteConfig
{
	private List<DataStoreType> writeOrder;
	private FailureAction failureAction = FailureAction.ABORT;

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
