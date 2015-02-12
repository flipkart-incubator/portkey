/**
 * 
 */
package com.flipkart.portkey.example.dao;

import java.util.Date;
import java.util.List;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsDataStore;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;
import com.flipkart.portkey.redis.metadata.annotation.RedisDataStore;
import com.flipkart.portkey.redis.metadata.annotation.RedisField;

/**
 * @author santosh.p
 */
@RdbmsDataStore (tableName = "Employee", databaseName = "", shardKeyField = "empId")
@RedisDataStore (primaryKeyPattern = "[CLASS]:{empId}:{aadharCardNumber}", secondaryKeyPatterns = "PAN:{panCardNumber}", shardKeyField = "empId")
public class Employee implements Entity
{
	@RdbmsField (columnName = "emp_id", isPrimaryKey = true)
	@RedisField ()
	private String empId;

	@RdbmsField (columnName = "aadhar_card_number", isPrimaryKey = true)
	@RedisField ()
	private String aadharCardNumber;

	@RdbmsField (columnName = "name")
	@RedisField ()
	private String name;

	@RdbmsField (columnName = "dob")
	@RedisField ()
	private Date dob;

	@RdbmsField (columnName = "pan_card_number")
	@RedisField ()
	private String panCardNumber;

	@RdbmsField (columnName = "address")
	@RedisField ()
	private Address address;

	@RdbmsField (columnName = "gender")
	@RedisField ()
	private Gender gender;

	@RdbmsField (columnName = "annual_salary")
	@RedisField ()
	private int annualSalary;

	@RdbmsField (columnName = "past_employers")
	@RedisField ()
	private List<String> pastEmployers;

	public String getEmpId()
	{
		return empId;
	}

	public void setEmpId(String empId)
	{
		this.empId = empId;
	}

	public String getAadharCardNumber()
	{
		return aadharCardNumber;
	}

	public void setAadharCardNumber(String aadharCardNumber)
	{
		this.aadharCardNumber = aadharCardNumber;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public Date getDob()
	{
		return dob;
	}

	public void setDob(Date dob)
	{
		this.dob = dob;
	}

	public String getPanCardNumber()
	{
		return panCardNumber;
	}

	public void setPanCardNumber(String panCardNumber)
	{
		this.panCardNumber = panCardNumber;
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

	public int getAnnualSalary()
	{
		return annualSalary;
	}

	public void setAnnualSalary(int annualSalary)
	{
		this.annualSalary = annualSalary;
	}

	public List<String> getPastEmployers()
	{
		return pastEmployers;
	}

	public void setPastEmployers(List<String> pastEmployers)
	{
		this.pastEmployers = pastEmployers;
	}

	@Override
	public String toString()
	{
		return "Employee [empId=" + empId + ", aadharCardNumber=" + aadharCardNumber + ", name=" + name + ", dob="
		        + dob + ", panCardNumber=" + panCardNumber + ", address=" + address + ", gender=" + gender
		        + ", annualSalary=" + annualSalary + ", pastEmployers=" + pastEmployers + "]";
	}
}
