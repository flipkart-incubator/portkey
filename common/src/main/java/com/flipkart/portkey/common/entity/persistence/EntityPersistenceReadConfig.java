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
	private List<DataStore> readOrderForKey;
	private List<DataStore> readOrderForNonKey;

	public List<DataStore> getReadOrderForKey()
	{
		return readOrderForKey;
	}

	public void setReadOrderForKey(List<DataStore> readOrderForKey)
	{
		this.readOrderForKey = readOrderForKey;
	}

	public List<DataStore> getReadOrderForNonKey()
	{
		return readOrderForNonKey;
	}

	public void setReadOrderForNonKey(List<DataStore> readOrderForNonKey)
	{
		this.readOrderForNonKey = readOrderForNonKey;
	}
}
