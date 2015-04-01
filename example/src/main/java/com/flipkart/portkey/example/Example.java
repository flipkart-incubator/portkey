/**
 * 
 */
package com.flipkart.portkey.example;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.example.dao.Address;
import com.flipkart.portkey.example.dao.Employee;
import com.flipkart.portkey.example.dao.Gender;
import com.flipkart.portkey.persistence.PersistenceLayer;

/**
 * @author santosh.p
 */
public class Example
{
	private static PersistenceLayer pl = null;

	public static void main(String[] args)
	{

		insert();
		insertWithGenerateId();
		upsert();
		updateBean();
		updateByCriteria();
		delete();
		getByCriteria();
		getAllAttributesByCriteria();
		getBeansBySql();
		getRsMapBySql();
	}

	private static Employee insertAndReturnBean()
	{
		PersistenceLayer pl = getPersistenceLayer();

		Employee emp = new Employee();
		emp.setEmpId("EE11111101");
		emp.setName("Some Name");
		emp.setDob(new Date());
		emp.setAnnualSalary(1234560);
		emp.setPanCardNumber("ABBPP738P");
		emp.setAadharCardNumber("3948ADH38A");
		emp.setGender(Gender.MALE);
		emp.setAddress(null);
		ArrayList<String> pastEmployers = new ArrayList<String>();
		pastEmployers.add("BlackStone");
		pastEmployers.add("Quora");
		pastEmployers.add("Coursera");
		emp.setPastEmployers(pastEmployers);

		try
		{
			pl.insert(emp);
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in insert: Exception while trying to insert bean" + e);
		}
		return emp;
	}

	private static PersistenceLayer getPersistenceLayer()
	{
		if (pl == null)
		{
			ApplicationContext context =
			        new FileSystemXmlApplicationContext("src/main/resources/external/portkey-application-context.xml");
			pl = context.getBean(PersistenceLayer.class, "persistenceLayer");
		}
		return pl;
	}

	private static void cleanUp()
	{
		PersistenceLayer pl = getPersistenceLayer();
		try
		{
			pl.getBySql("DELETE FROM Employee", null);
		}
		catch (PortKeyException e)
		{
			System.out.println("Exception while cleaning the table:" + e.toString());
		}
		System.out.println("Cleaned up the table");
	}

	private static List<Employee> getAllBeansFromTable()
	{
		PersistenceLayer pl = getPersistenceLayer();
		List<Employee> l = null;
		try
		{
			l = pl.getBySql(Employee.class, "SELECT * FROM Employee", null);
		}
		catch (PortKeyException e)
		{
			System.out.println("Exception while trying to retrieve all rows from table:" + e.toString());
		}
		return l;
	}

	private static void insert()
	{
		System.out.println("Testing insert method");
		cleanUp();
		PersistenceLayer pl = getPersistenceLayer();

		Employee emp = new Employee();
		emp.setEmpId("EE11111101");
		emp.setName("Some Name");
		emp.setDob(new Date());
		emp.setAnnualSalary(1234560);
		emp.setPanCardNumber("ABBPP738P");
		emp.setAadharCardNumber("3948ADH38A");
		emp.setGender(Gender.MALE);
		emp.setAddress(null);
		ArrayList<String> pastEmployers = new ArrayList<String>();
		pastEmployers.add("BlackStone");
		pastEmployers.add("Quora");
		pastEmployers.add("Coursera");
		emp.setPastEmployers(pastEmployers);

		try
		{
			Result r = pl.insert(emp);
			Employee returned = (Employee) r.getEntity();
			if (!returned.equals(emp))
			{
				System.out.println("Failure in insert: Bean returned by insert method does not match with passed one");
				System.out.println("Passed:" + emp);
				System.out.println("Returned:" + returned);
			}
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in insert: Exception while trying to insert bean" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			System.out.println("Failure in insert: No rows are returned by query");
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				System.out.println("Failure in insert:Inserted bean doesn't match with fetched bean");
			}
		}
		else
		{
			System.out.println("Failure in insert:Inserted only one bean but query returned more");
		}
		System.out.println("Success!");
	}

	private static void insertWithGenerateId()
	{
		System.out.println("Testing insert method with generate id");

		cleanUp();

		PersistenceLayer pl = getPersistenceLayer();

		Employee emp = new Employee();
		emp.setEmpId("EE11111101");
		emp.setName("Some Other Name");
		emp.setDob(new Date());
		emp.setAnnualSalary(7363826);
		emp.setPanCardNumber("ABUHH738P");
		emp.setAadharCardNumber("3A87HH28A");
		emp.setGender(Gender.FEMALE);
		Address address = new Address();
		address.setLine1("Flipkart Internet Pvt Ltd");
		address.setLine2("24, Baker Street");
		address.setArea("Devarabisanahalli");
		address.setCity("Bangalore");
		address.setPinCode(560103);
		emp.setAddress(address);
		ArrayList<String> pastEmployers = new ArrayList<String>();
		pastEmployers.add("AT&T");
		pastEmployers.add("TAG Heuer");
		pastEmployers.add("20th Century Fox");
		emp.setPastEmployers(pastEmployers);

		try
		{
			Result r = pl.insert(emp, true);
			Employee returned = (Employee) r.getEntity();
			emp.setEmpId(emp.getEmpId() + "01");
			if (!returned.equals(emp))
			{
				System.out
				        .println("Failure in insertWithGenerateId: Bean returned by insert method does not match with passed one");
				System.out.println("Passed:" + emp);
				System.out.println("Returned:" + returned);
			}
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in insertWithGenerateId: Exception while trying to insert bean" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			System.out.println("Failure in insertWithGenerateId: No rows are returned by query");
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				System.out.println("Failure in insertWithGenerateId: bean doesn't match with fetched bean");
			}
		}
		else
		{
			System.out.println("Failure in insertWithGenerateId: Inserted only one bean but query returned more");
		}
		System.out.println("Success!");
	}

	private static void upsert()
	{
		System.out.println("Testing upsert method with generate id");

		cleanUp();

		PersistenceLayer pl = getPersistenceLayer();

		Employee emp = new Employee();
		emp.setEmpId("EE11111101");
		emp.setName("Some Other Name");
		emp.setDob(new Date());
		emp.setAnnualSalary(7363826);
		emp.setPanCardNumber("ABUHH738P");
		emp.setAadharCardNumber("3A87HH28A");
		emp.setGender(Gender.FEMALE);
		Address address = new Address();
		address.setLine1("Flipkart Internet Pvt Ltd");
		address.setLine2("24, Baker Street");
		address.setArea("Devarabisanahalli");
		address.setCity("Bangalore");
		address.setPinCode(560103);
		emp.setAddress(address);
		ArrayList<String> pastEmployers = new ArrayList<String>();
		pastEmployers.add("AT&T");
		pastEmployers.add("TAG Heuer");
		pastEmployers.add("20th Century Fox");
		emp.setPastEmployers(pastEmployers);

		try
		{
			Result r = pl.upsert(emp);
			Employee returned = (Employee) r.getEntity();
			if (!returned.equals(emp))
			{
				System.out.println("Failure in upsert: Bean returned by upsert method does not match with passed one");
				System.out.println("Passed:" + emp);
				System.out.println("Returned:" + returned);
			}
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in upsert: Exception while trying to upsert bean" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			System.out.println("Failure in upsert: No rows are returned by query");
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				System.out.println("Failure in upsert: bean doesn't match with fetched bean");
			}
		}
		else
		{
			System.out.println("Failure in upsert: Inserted only one bean but query returned more");
		}
		System.out.println("Success!");
	}

	private static void updateBean()
	{
		System.out.println("Testing updateBean");
		cleanUp();
		Employee emp = insertAndReturnBean();
		emp.setName(emp.getName() + " Some Surname");
		PersistenceLayer pl = getPersistenceLayer();
		try
		{
			Result r = pl.update(emp);
			Employee returned = (Employee) r.getEntity();
			if (!returned.equals(emp))
			{
				System.out.println("Failure in insert: Bean returned by insert method does not match with passed one");
				System.out.println("Passed:" + emp);
				System.out.println("Returned:" + returned);
			}
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in updateBean: Exception while trying to update bean" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			System.out.println("Failure in updateBean: No rows are returned by query");
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				System.out.println("Failure in updateBean: Inserted bean doesn't match with fetched bean");
			}
		}
		else
		{
			System.out.println("Failure in updateBean: Inserted only one bean but query returned more");
		}
		System.out.println("Success!");
	}

	private static void updateByCriteria()
	{
		System.out.println("Testing updateByCriteria");
		cleanUp();
		Employee emp = insertAndReturnBean();
		PersistenceLayer pl = getPersistenceLayer();
		try
		{
			Map<String, Object> updateValuesMap = new HashMap<String, Object>();
			List<String> pastEmployers = emp.getPastEmployers();
			pastEmployers.add("University of Washington");
			emp.setPastEmployers(pastEmployers);
			updateValuesMap.put("pastEmployers", pastEmployers);
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("panCardNumber", emp.getPanCardNumber());
			pl.update(Employee.class, updateValuesMap, criteria);
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in updateByCriteria: Exception while trying to update bean:" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			System.out.println("Failure in updateByCriteria: No rows are returned by query");
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				System.out.println("Failure in updateByCriteria: Inserted bean doesn't match with fetched bean");
				System.out.println("Inserted:" + emp);
				System.out.println("Returned:" + l.get(0));
			}
		}
		else
		{
			System.out.println("Failure in updateByCriteria: Inserted only one bean but query returned more");
		}
		System.out.println("Success!");
	}

	private static void delete()
	{
		System.out.println("Testing delete");
		cleanUp();
		Employee emp = insertAndReturnBean();
		PersistenceLayer pl = getPersistenceLayer();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("gender", emp.getGender());
			pl.delete(Employee.class, criteria);
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in delete: Exception while trying to delete bean:" + e);
		}
		List<Employee> l = getAllBeansFromTable();
		if (l != null && l.size() != 0)
		{
			System.out.println("Failure in delete: Non zero number of rows are returned by query");
		}
		System.out.println("Success!");
	}

	private static void getByCriteria()
	{
		System.out.println("Testing getByCriteria");
		cleanUp();
		Employee emp = insertAndReturnBean();
		PersistenceLayer pl = getPersistenceLayer();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("address", emp.getAddress());
			List<String> attributes = new ArrayList<String>();
			attributes.add("name");
			attributes.add("dob");
			attributes.add("annualSalary");
			Employee expected = new Employee();
			expected.setName(emp.getName());
			expected.setDob(emp.getDob());
			expected.setAnnualSalary(emp.getAnnualSalary());
			List<Employee> l = pl.getByCriteria(Employee.class, attributes, criteria);
			if (l == null || l.size() == 0)
			{
				System.out.println("Failure in getByCriteria: No rows are returned by query");
			}
			else if (l.size() == 1)
			{
				if (!expected.equals(l.get(0)))
				{
					System.out.println("Failure in getByCriteria: Inserted bean doesn't match with fetched bean");
					System.out.println("Expected:" + emp);
					System.out.println("Returned:" + l.get(0));
				}
			}
			else
			{
				System.out.println("Failure in getByCriteria: Inserted only one bean but query returned more");
			}
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in getByCriteria: Exception while trying to delete bean:" + e);
		}
		System.out.println("Success!");
	}

	private static void getAllAttributesByCriteria()
	{
		System.out.println("Testing getAllAttributesByCriteria");
		cleanUp();
		Employee emp = insertAndReturnBean();
		PersistenceLayer pl = getPersistenceLayer();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("address", emp.getAddress());
			List<Employee> l = pl.getByCriteria(Employee.class, criteria);
			if (l == null || l.size() == 0)
			{
				System.out.println("Failure in getAllAttributesByCriteria: No rows are returned by query");
			}
			else if (l.size() == 1)
			{
				if (!emp.equals(l.get(0)))
				{
					System.out
					        .println("Failure in getAllAttributesByCriteria: Inserted bean doesn't match with fetched bean");
					System.out.println("Inserted:" + emp);
					System.out.println("Returned:" + l.get(0));
				}
			}
			else
			{
				System.out
				        .println("Failure in getAllAttributesByCriteria: Inserted only one bean but query returned more");
			}
		}
		catch (PortKeyException e)
		{
			System.out.println("Failure in getAllAttributesByCriteria: Exception while trying to delete bean:" + e);
		}
		System.out.println("Success!");
	}

	private static void getBeansBySql()
	{
		System.out.println("Testing getBeansBySql");
		cleanUp();
		Employee emp = insertAndReturnBean();
		PersistenceLayer pl = getPersistenceLayer();
		List<Employee> l = null;
		try
		{
			l = pl.getBySql(Employee.class, "SELECT * FROM Employee", null);
			if (l == null || l.size() == 0)
			{
				System.out.println("Failure in getBeansBySql: No rows are returned by query");
			}
			else if (l.size() == 1)
			{
				if (!emp.equals(l.get(0)))
				{
					System.out.println("Failure in getBeansBySql: Inserted bean doesn't match with fetched bean");
					System.out.println("Inserted:" + emp);
					System.out.println("Returned:" + l.get(0));
				}
			}
			else
			{
				System.out.println("Failure in getBeansBySql: Inserted only one bean but query returned more");
			}
		}
		catch (PortKeyException e)
		{
			System.out.println("Exception while trying to retrieve all rows from table:" + e.toString());
		}
		System.out.println("Success!");
	}

	private static void getRsMapBySql()
	{
		System.out.println("Testing getRsMapBySql");
		cleanUp();
		Employee emp = insertAndReturnBean();
		PersistenceLayer pl = getPersistenceLayer();
		List<Map<String, Object>> tupleList = null;
		try
		{
			tupleList = pl.getBySql("SELECT * FROM Employee", null);
			if (tupleList == null || tupleList.size() == 0)
			{
				System.out.println("Failure in getBeansBySql: No rows are returned by query");
			}
			else if (tupleList.size() == 1)
			{
				Map<String, Object> columnToValueMap = tupleList.get(0);
				Field[] fields = Employee.class.getFields();
				for (Field field : fields)
				{
					field.setAccessible(true);
					String fieldName = field.getName();
					if (!columnToValueMap.get(fieldName).equals(field.get(emp)))
					{
						System.out
						        .println("Failure in getRsMapBySql: Inconsistent values for field " + field.getName());
						System.out.println("Expected:" + field.get(emp));
						System.out.println("Received:" + columnToValueMap.get(fieldName));
					}
				}
				if (!emp.equals(columnToValueMap.get(0)))
				{
					System.out.println("Failure in getBeansBySql: Inserted bean doesn't match with fetched bean");
					System.out.println("Inserted:" + emp);
					System.out.println("Returned:" + columnToValueMap.get(0));
				}
			}
			else
			{
				System.out.println("Failure in getBeansBySql: Inserted only one bean but query returned more");
			}
		}
		catch (PortKeyException e)
		{
			System.out.println("Exception while trying to retrieve all rows from table:" + e.toString());
		}
		catch (IllegalArgumentException e)
		{
			System.out.println("Exception while trying to retrieve all rows from table:" + e.toString());
		}
		catch (IllegalAccessException e)
		{
			System.out.println("Exception while trying to retrieve all rows from table:" + e.toString());
		}
		System.out.println("Success!");
	}
}
