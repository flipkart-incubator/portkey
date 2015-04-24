package com.flipkart.portkey.rdbms.dao;

import java.sql.Timestamp;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsDataStore;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;

@RdbmsDataStore (databaseName = "test_db", shardKeyField = "id", tableName = "person")
public class Person implements Entity
{
	@RdbmsField (columnName = "id", isPrimaryKey = true)
	private Long id;
	@RdbmsField (columnName = "first_name")
	private String firstName;
	@RdbmsField (columnName = "last_name")
	private String lastName;
	@RdbmsField (columnName = "age")
	private int age;
	@RdbmsField (columnName = "mod_count", defaultValue = "mod_count+1")
	private Timestamp modCount;
	@RdbmsField (columnName = "last_modified", defaultValue = "now()")
	private Timestamp lastModified;

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public String getFirstName()
	{
		return firstName;
	}

	public void setFirstName(String firstName)
	{
		this.firstName = firstName;
	}

	public String getLastName()
	{
		return lastName;
	}

	public void setLastName(String lastName)
	{
		this.lastName = lastName;
	}

	public int getAge()
	{
		return age;
	}

	public void setAge(int age)
	{
		this.age = age;
	}

	public Timestamp getLastModified()
	{
		return lastModified;
	}

	public Timestamp getModCount()
	{
		return modCount;
	}

	public void setModCount(Timestamp modCount)
	{
		this.modCount = modCount;
	}

	public void setLastModified(Timestamp lastModified)
	{
		this.lastModified = lastModified;
	}
}
