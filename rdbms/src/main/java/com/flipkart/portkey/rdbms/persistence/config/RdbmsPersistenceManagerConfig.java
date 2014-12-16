/**
 * 
 */
package com.flipkart.portkey.rdbms.persistence.config;

import java.util.List;

import javax.sql.DataSource;

/**
 * @author santosh.p
 */
public class RdbmsPersistenceManagerConfig
{
	DataSource master;
	List<DataSource> slaves;

	public DataSource getMaster()
	{
		return master;
	}

	public void setMaster(DataSource master)
	{
		this.master = master;
	}

	public List<DataSource> getSlaves()
	{
		return slaves;
	}

	public void setSlaves(List<DataSource> slaves)
	{
		this.slaves = slaves;
	}

	public void addToSlaves(DataSource slave)
	{
		this.slaves.add(slave);
	}

}
