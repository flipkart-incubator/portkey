/**
 * 
 */
package com.flipkart.portkey.example;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.example.dao.Employee;
import com.flipkart.portkey.example.dao.Gender;
import com.flipkart.portkey.persistence.PersistenceLayer;

/**
 * @author santosh.p
 */
public class Example
{
	private static Logger logger = Logger.getLogger(Example.class);
	private static PersistenceLayer pl = null;

	public static void main(String[] args) throws IllegalAccessException, InvocationTargetException,
	        NoSuchMethodException
	{
		ApplicationContext context =
		        new FileSystemXmlApplicationContext("src/main/resources/external/portkey-application-context.xml");
		pl = context.getBean(PersistenceLayer.class, "persistenceLayer");
		// pl = PortKeyInitializer.initialize();
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
		System.exit(0);
	}

	private static Employee createEmployee()
	{
		Employee emp = new Employee();
		emp.setEmpId("EE11111101");
		emp.setName("Some Name");
		emp.setStampCreated((new Timestamp(new Date().getTime())));
		emp.setAnnualSalary(1234560);
		emp.setPanCardNumber("ABBPP738P");
		emp.setAadharCardNumber("3948ADH38A");
		emp.setGender(Gender.MALE);
		emp.setAddress(null);
		ArrayList<String> pastEmployers = new ArrayList<String>();
		pastEmployers.add("AT&T");
		pastEmployers.add("TAG Heuer");
		pastEmployers.add("20th Century Fox");
		emp.setPastEmployers(pastEmployers);
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		Date joiningDate = null;
		try
		{
			joiningDate = sdf.parse("21/12/2012");
		}
		catch (ParseException e1)
		{
			logger.warn("Exception while parsing date");
		}
		emp.setJoiningDate(joiningDate);

		return emp;
	}

	private static Employee insertAndReturnBean()
	{
		Employee emp = createEmployee();
		try
		{
			pl.insert(emp);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in insert: Exception while trying to insert bean" + e);
		}
		return emp;
	}

	private static void cleanUp()
	{
		try
		{
			pl.updateBySql("DELETE FROM Employee", null);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while cleaning the table:" + e.toString());
		}
		logger.info("Cleaned up the table");
	}

	private static List<Employee> getAllBeansFromTable()
	{
		List<Employee> l = null;
		try
		{
			l = pl.getBySql(Employee.class, "SELECT * FROM Employee", null);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to retrieve all rows from table:" + e.toString());
		}
		return l;
	}

	private static void insert()
	{
		logger.info("Testing insert method");
		cleanUp();

		Employee emp = createEmployee();

		try
		{
			Result r = pl.insert(emp);
			Employee returned = (Employee) r.getEntity();
			if (!returned.equals(emp))
			{
				logger.info("Failure in insert: Bean returned by insert method does not match with passed one");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in insert: Exception while trying to insert bean" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in insert: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			Employee returned = l.get(0);
			if (!emp.equals(returned))
			{
				logger.info("Failure in insert:Inserted bean doesn't match with fetched bean");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		else
		{
			logger.info("Failure in insert:Inserted only one bean but query returned more");
			return;
		}
		logger.info("Success!");
	}

	private static void insertWithGenerateId()
	{
		logger.info("Testing insert method with generate id");
		cleanUp();
		Employee emp = createEmployee();
		try
		{
			Result r = pl.insert(emp, true);
			Employee returned = (Employee) r.getEntity();
			if (!returned.equals(emp))
			{
				System.out
				        .println("Failure in insertWithGenerateId: Bean returned by insert method does not match with passed one");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in insertWithGenerateId: Exception while trying to insert bean" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in insertWithGenerateId: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			Employee returned = l.get(0);
			if (!emp.equals(l.get(0)))
			{
				logger.info("Failure in insertWithGenerateId: bean doesn't match with fetched bean");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		else
		{
			logger.info("Failure in insertWithGenerateId: Inserted only one bean but query returned more");
			return;
		}
		logger.info("Success!");
	}

	private static void upsert()
	{
		logger.info("Testing upsert method with generate id");
		cleanUp();
		Employee emp = createEmployee();
		try
		{
			Result r = pl.upsert(emp);
			Employee returned = (Employee) r.getEntity();
			if (!returned.equals(emp))
			{
				logger.info("Failure in upsert: Bean returned by upsert method does not match with passed one");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in upsert: Exception while trying to upsert bean" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in upsert: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				logger.info("Failure in upsert: bean doesn't match with fetched bean");
				return;
			}
		}
		else
		{
			logger.info("Failure in upsert: Inserted only one bean but query returned more");
			return;
		}
		logger.info("Success!");
	}

	private static void updateBean()
	{
		logger.info("Testing updateBean");
		cleanUp();
		Employee emp = insertAndReturnBean();
		emp.setName(emp.getName() + " Some Surname");
		try
		{
			Result r = pl.update(emp);
			Employee returned = (Employee) r.getEntity();
			if (!returned.equals(emp))
			{
				logger.info("Failure in insert: Bean returned by insert method does not match with passed one");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in updateBean: Exception while trying to update bean" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in updateBean: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				logger.info("Failure in updateBean: Inserted bean doesn't match with fetched bean");
				return;
			}
		}
		else
		{
			logger.info("Failure in updateBean: Inserted only one bean but query returned more");
			return;
		}
		logger.info("Success!");
	}

	private static void updateByCriteria()
	{
		logger.info("Testing updateByCriteria");
		cleanUp();
		Employee emp = insertAndReturnBean();
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
			logger.info("Failure in updateByCriteria: Exception while trying to update bean:" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in updateByCriteria: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				logger.info("Failure in updateByCriteria: Inserted bean doesn't match with fetched bean");
				logger.info("Inserted:" + emp);
				logger.info("Returned:" + l.get(0));
				return;
			}
		}
		else
		{
			logger.info("Failure in updateByCriteria: Inserted only one bean but query returned more");
			return;
		}
		logger.info("Success!");
	}

	private static void delete()
	{
		logger.info("Testing delete");
		cleanUp();
		Employee emp = insertAndReturnBean();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("gender", emp.getGender());
			pl.delete(Employee.class, criteria);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in delete: Exception while trying to delete bean:" + e);
			return;
		}
		List<Employee> l = getAllBeansFromTable();
		if (l != null && l.size() != 0)
		{
			logger.info("Failure in delete: Non zero number of rows are returned by query");
			return;
		}
		logger.info("Success!");
	}

	private static void getByCriteria()
	{
		logger.info("Testing getByCriteria");
		cleanUp();
		Employee emp = insertAndReturnBean();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("address", emp.getAddress());
			List<String> attributes = new ArrayList<String>();
			attributes.add("name");
			attributes.add("joiningDate");
			attributes.add("annualSalary");
			Employee expected = new Employee();
			expected.setName(emp.getName());
			expected.setAnnualSalary(emp.getAnnualSalary());
			expected.setJoiningDate(emp.getJoiningDate());
			List<Employee> l = pl.getByCriteria(Employee.class, attributes, criteria);
			if (l == null || l.size() == 0)
			{
				logger.info("Failure in getByCriteria: No rows are returned by query");
				return;
			}
			else if (l.size() == 1)
			{
				if (!expected.equals(l.get(0)))
				{
					logger.info("Failure in getByCriteria: Inserted bean doesn't match with fetched bean");
					logger.info("Expected:" + emp);
					logger.info("Returned:" + l.get(0));
					return;
				}
			}
			else
			{
				logger.info("Failure in getByCriteria: Inserted only one bean but query returned more");
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in getByCriteria: Exception while trying to delete bean:" + e);
			return;
		}
		logger.info("Success!");
	}

	private static void getAllAttributesByCriteria()
	{
		logger.info("Testing getAllAttributesByCriteria");
		cleanUp();
		Employee emp = insertAndReturnBean();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("address", emp.getAddress());
			List<Employee> l = pl.getByCriteria(Employee.class, criteria);
			if (l == null || l.size() == 0)
			{
				logger.info("Failure in getAllAttributesByCriteria: No rows are returned by query");
				return;
			}
			else if (l.size() == 1)
			{
				if (!emp.equals(l.get(0)))
				{
					System.out
					        .println("Failure in getAllAttributesByCriteria: Inserted bean doesn't match with fetched bean");
					logger.info("Inserted:" + emp);
					logger.info("Returned:" + l.get(0));
					return;
				}
			}
			else
			{
				logger.info("Failure in getAllAttributesByCriteria: Inserted only one bean but query returned more");
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in getAllAttributesByCriteria: Exception while trying to delete bean:" + e);
			return;
		}
		logger.info("Success!");
	}

	private static void getBeansBySql()
	{
		logger.info("Testing getBeansBySql");
		cleanUp();
		Employee emp = insertAndReturnBean();
		List<Employee> l = null;
		try
		{
			l = pl.getBySql(Employee.class, "SELECT * FROM Employee", null);
			if (l == null || l.size() == 0)
			{
				logger.info("Failure in getBeansBySql: No rows are returned by query");
				return;
			}
			else if (l.size() == 1)
			{
				if (!emp.equals(l.get(0)))
				{
					logger.info("Failure in getBeansBySql: Inserted bean doesn't match with fetched bean");
					logger.info("Inserted:" + emp);
					logger.info("Returned:" + l.get(0));
					return;
				}
			}
			else
			{
				logger.info("Failure in getBeansBySql: Inserted only one bean but query returned more");
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to retrieve all rows from table:" + e.toString());
			return;
		}
		logger.info("Success!");
	}

	private static void getRsMapBySql()
	{
		logger.info("Testing getRsMapBySql");
		cleanUp();
		Employee emp = insertAndReturnBean();
		List<Map<String, Object>> tupleList = null;
		try
		{
			tupleList = pl.getBySql("SELECT * FROM Employee", null);
			if (tupleList == null || tupleList.size() == 0)
			{
				logger.info("Failure in getBeansBySql: No rows are returned by query");
				return;
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
						logger.info("Failure in getRsMapBySql: Inconsistent values for field " + field.getName());
						logger.info("Expected:" + field.get(emp));
						logger.info("Received:" + columnToValueMap.get(fieldName));
						return;
					}
				}
			}
			else
			{
				logger.info("Failure in getBeansBySql: Inserted only one bean but query returned more");
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to retrieve all rows from table:" + e.toString());
			return;
		}
		catch (IllegalArgumentException e)
		{
			logger.info("Exception while trying to retrieve all rows from table:" + e.toString());
			return;
		}
		catch (IllegalAccessException e)
		{
			logger.info("Exception while trying to retrieve all rows from table:" + e.toString());
			return;
		}
		logger.info("Success!");
	}
}
