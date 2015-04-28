/**
 * 
 */
package com.flipkart.portkey.example.dao;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsDataStore;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;
import com.flipkart.portkey.redis.metadata.annotation.RedisDataStore;
import com.flipkart.portkey.redis.metadata.annotation.RedisField;

/**
 * @author santosh.p
 */
@RdbmsDataStore (tableName = "employee", databaseName = "portkey_example", shardKeyField = "empId")
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

	@RdbmsField (columnName = "joining_date")
	@RedisField ()
	private Date joiningDate;

	@RdbmsField (columnName = "stamp_created")
	@RedisField ()
	private Timestamp stampCreated;

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

	public Date getJoiningDate()
	{
		return joiningDate;
	}

	public void setJoiningDate(Date joiningDate)
	{
		this.joiningDate = joiningDate;
	}

	public Timestamp getStampCreated()
	{
		return stampCreated;
	}

	public void setStampCreated(Timestamp stampCreated)
	{
		this.stampCreated = stampCreated;
	}

	@Override
	public String toString()
	{
		return "Employee [empId=" + empId + ", aadharCardNumber=" + aadharCardNumber + ", name=" + name
		        + ", panCardNumber=" + panCardNumber + ", address=" + address + ", gender=" + gender
		        + ", annualSalary=" + annualSalary + ", pastEmployers=" + pastEmployers + ", joiningDate="
		        + joiningDate + ", stampCreated=" + stampCreated + "]";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((aadharCardNumber == null) ? 0 : aadharCardNumber.hashCode());
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result + annualSalary;
		result = prime * result + ((empId == null) ? 0 : empId.hashCode());
		result = prime * result + ((gender == null) ? 0 : gender.hashCode());
		result = prime * result + ((joiningDate == null) ? 0 : joiningDate.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((panCardNumber == null) ? 0 : panCardNumber.hashCode());
		result = prime * result + ((pastEmployers == null) ? 0 : pastEmployers.hashCode());
		result =
		        prime * result
		                + ((stampCreated == null) ? 0 : DateUtils.round(stampCreated, Calendar.SECOND).hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Employee other = (Employee) obj;
		if (aadharCardNumber == null)
		{
			if (other.aadharCardNumber != null)
				return false;
		}
		else if (!aadharCardNumber.equals(other.aadharCardNumber))
			return false;
		if (address == null)
		{
			if (other.address != null)
				return false;
		}
		else if (!address.equals(other.address))
			return false;
		if (annualSalary != other.annualSalary)
			return false;
		if (empId == null)
		{
			if (other.empId != null)
				return false;
		}
		else if (!empId.equals(other.empId))
			return false;
		if (gender != other.gender)
			return false;
		if (joiningDate == null)
		{
			if (other.joiningDate != null)
				return false;
		}
		else if (!joiningDate.equals(other.joiningDate))
			return false;
		if (name == null)
		{
			if (other.name != null)
				return false;
		}
		else if (!name.equals(other.name))
			return false;
		if (panCardNumber == null)
		{
			if (other.panCardNumber != null)
				return false;
		}
		else if (!panCardNumber.equals(other.panCardNumber))
			return false;
		if (pastEmployers == null)
		{
			if (other.pastEmployers != null)
				return false;
		}
		else if (!pastEmployers.equals(other.pastEmployers))
			return false;
		if (stampCreated == null)
		{
			if (other.stampCreated != null)
				return false;
		}
		else if (!DateUtils.round(stampCreated, Calendar.SECOND).equals(
		        DateUtils.round(other.stampCreated, Calendar.SECOND)))
			return false;
		return true;
	}
}
