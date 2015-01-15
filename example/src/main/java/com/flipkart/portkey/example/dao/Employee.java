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
@RdbmsTable (tableName = "Employee")
@RedisDataStore (primaryKeyPattern = "[CLASS]:{empId}:{aadharCardNumber}", secondaryKeyPatterns = "PAN:{panCardNumber}")
public class Employee implements Entity
{
	@RdbmsField (columnName = "emp_id", isPrimaryKey = true, isShardKey = true)
	@RedisField (attributeName = "emp_id", isShardKey = true)
	private String empId;

	@RdbmsField (columnName = "aadhar_card_number", isPrimaryKey = true)
	@RedisField (attributeName = "aadhar_card_number")
	private String aadharCardNumber;

	@RdbmsField (columnName = "name")
	@RedisField (attributeName = "name")
	private String name;

	@RdbmsField (columnName = "dob")
	@RedisField (attributeName = "dob")
	private Date dob;

	@RdbmsField (columnName = "pan_card_number")
	@RedisField (attributeName = "pan_card_number")
	private String panCardNumber;

	@RdbmsField (columnName = "address")
	@RedisField (attributeName = "address", isJson = true)
	private Address address;

	@RdbmsField (columnName = "gender")
	@RedisField (attributeName = "gender")
	private Gender gender;

	@RdbmsField (columnName = "annual_salary")
	@RedisField (attributeName = "annual_salary")
	private int annualSalary;

	@RdbmsField (columnName = "past_employers")
	@RedisField (attributeName = "past_employers", isJsonList = true)
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
