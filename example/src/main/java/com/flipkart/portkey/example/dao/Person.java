/**
 * 
 */
package com.flipkart.portkey.example.dao;

import java.util.Date;
import java.util.List;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsTable;
import com.flipkart.portkey.redis.metadata.annotation.RedisDataStore;
import com.flipkart.portkey.redis.metadata.annotation.RedisField;

/**
 * @author santosh.p
 */
@RdbmsTable (tableName = "Person")
@RedisDataStore (primaryKeyPattern = "[CLASS]:{id}")
public class Person implements Entity
{

	@RdbmsField (columnName = "id", isPrimaryKey = true, isShardKey = true)
	@RedisField (attributeName = "id", isShardKey = true)
	private String id;

	@RdbmsField (columnName = "name")
	@RedisField (attributeName = "name")
	private String name;

	@RdbmsField (columnName = "last_login")
	@RedisField (attributeName = "last_login")
	private Date lastLogin;

	@RdbmsField (columnName = "address")
	@RedisField (attributeName = "address", isJson = true)
	private Address address;

	@RdbmsField (columnName = "gender")
	@RedisField (attributeName = "gender")
	private Gender gender;

	@RdbmsField (columnName = "annual_income")
	@RedisField (attributeName = "annual_income")
	private int annualIncome;

	@RdbmsField (columnName = "places_visited")
	@RedisField (attributeName = "places_visited", isJsonList = true)
	private List<String> placesVisited;

	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public Date getLastLogin()
	{
		return lastLogin;
	}

	public void setLastLogin(Date lastSeen)
	{
		this.lastLogin = lastSeen;
	}

	public Address getAddress()
	{
		return address;
	}

	public void setAddress(Address address)
	{
		this.address = address;
	}

	public Gender getGender()
	{
		return gender;
	}

	public void setGender(Gender gender)
	{
		this.gender = gender;
	}

	public int getAnnualIncome()
	{
		return annualIncome;
	}

	public void setAnnualIncome(int annualIncome)
	{
		this.annualIncome = annualIncome;
	}

	public List<String> getPlacesVisited()
	{
		return placesVisited;
	}

	public void setPlacesVisited(List<String> placesVisited)
	{
		this.placesVisited = placesVisited;
	}

	@Override
	public String toString()
	{
		return "Person [id=" + id + ", name=" + name + ", lastLogin=" + lastLogin + ", address=" + address.toString()
		        + ", gender=" + gender + ", annualIncome=" + annualIncome + ", placesVisited=" + placesVisited + "]";
	}
}
