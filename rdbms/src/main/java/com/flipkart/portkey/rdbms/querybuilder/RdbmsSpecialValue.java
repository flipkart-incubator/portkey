package com.flipkart.portkey.rdbms.querybuilder;

public class RdbmsSpecialValue
{
	private Object value;

	public RdbmsSpecialValue()
	{

	}

	public RdbmsSpecialValue(String value)
	{
		this.value = value;
	}

	public Object getValue()
	{
		return value;
	}

	public void setValue(Object value)
	{
		this.value = value;
	}

	@Override
	public String toString()
	{
		if (value == null)
		{
			return null;
		}
		return value.toString();
	}
}
