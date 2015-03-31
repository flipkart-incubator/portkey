package com.flipkart.portkey.common.persistence.query;

import com.flipkart.portkey.common.entity.Entity;

public class InsertQuery implements PortKeyQuery
{
	Entity bean;

	public Entity getBean()
	{
		return bean;
	}

	public void setBean(Entity bean)
	{
		this.bean = bean;
	}
}
