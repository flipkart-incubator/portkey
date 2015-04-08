package com.flipkart.portkey.common.persistence.query;

import java.util.List;

import com.flipkart.portkey.common.entity.Entity;

public class UpsertQuery implements PortKeyQuery
{
	Entity bean;
	List<String> updateFields;

	public Entity getBean()
	{
		return bean;
	}

	public void setBean(Entity bean)
	{
		this.bean = bean;
	}

	public List<String> getUpdateFields()
	{
		return updateFields;
	}

	public void setUpdateFields(List<String> updateFields)
	{
		this.updateFields = updateFields;
	}

	public void addUpdateField(String updateField)
	{
		this.updateFields.add(updateField);
	}
}
