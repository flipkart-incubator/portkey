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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.common.exception.QueryExecutionException;
import com.flipkart.portkey.common.persistence.Result;
import com.flipkart.portkey.common.persistence.Row;
import com.flipkart.portkey.common.persistence.TransactionManager;
import com.flipkart.portkey.common.persistence.query.UpdateQuery;
import com.flipkart.portkey.example.dao.Employee;
import com.flipkart.portkey.example.dao.EmployeeSharded;
import com.flipkart.portkey.example.dao.Gender;
import com.flipkart.portkey.persistence.PersistenceLayer;

/**
 * @author santosh.p
 */
public class Example
{
	private static Logger logger = LogManager.getLogger(Example.class);
	private static PersistenceLayer pl = null;

	@SuppressWarnings ("resource")
	public static void main(String[] args) throws IllegalAccessException, InvocationTargetException,
	        NoSuchMethodException, PortKeyException
	{
		ApplicationContext context =
		        new FileSystemXmlApplicationContext("src/main/resources/external/portkey-application-context.xml");
		pl = context.getBean(PersistenceLayer.class, "persistenceLayer");
		cleanUp();
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
		updateTransactionalSuccess();
		updateTransactionalFailure();
		updateTransactionalSuccessWithNoRowsUpdate();
		updateTransactionalFailureWithNoRowsUpdate();
		insertTransactionalSuccess();
		insertTransactionalFailure();
		transactionalSuccess();
		transactionalFailure();
		cleanUp();

		// test sharded
		insert2();
		insertWithGenerateId2();
		upsert2();
		updateBean2();
		updateByCriteria2();
		delete2();
		getByCriteria2();
		getAllAttributesByCriteria2();
		getBeansBySql2();
		getRsMapBySql2();
		cleanUp2();
		logger.info("Success!!");
		System.exit(0);
	}

	private static void transactionalSuccess()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		Employee emp = createEmployee();
		TransactionManager tm;
		try
		{
			tm = pl.getTransactionManager(emp, DataStoreType.RDBMS);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in transactionalSuccess: Exception while trying to get transaction manager instance");
			return;
		}
		tm.begin();
		try
		{
			tm.insert(emp);
		}
		catch (QueryExecutionException e)
		{
			logger.info("Failure in transactionalSuccess: Exception while trying to insert pojo");
			return;
		}
		Employee emp2 = createEmployee();
		try
		{
			tm.insert(emp2);
		}
		catch (Exception e)
		{
			logger.info("Failure in transactionalSuccess: Exception while trying to insert pojo");
			return;
		}
		try
		{
			List<Employee> r = tm.getByCriteria(Employee.class, new HashMap<String, Object>());
			if (r.size() != 2)
			{
				logger.info("Failure in transactionalSuccess: Expected two rows received " + r.size());
				return;
			}
			if (!(r.get(0).equals(emp) && r.get(1).equals(emp2) || r.get(0).equals(emp2) && r.get(1).equals(emp)))
			{
				logger.info("Failure in transactionalSuccess: expected !=received, expected=" + emp + " and " + emp2
				        + ", received=" + r.get(0) + " and " + r.get(1));
			}
		}
		catch (QueryExecutionException e)
		{
			logger.info("Failure in transactionalSuccess: Exception while trying to get result from db");
			return;
		}
		try
		{
			tm.commit();
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in transactionalSuccess: Exception while trying commit");
			return;
		}
		try
		{
			List<Employee> r = pl.getByCriteria(Employee.class, new HashMap<String, Object>());
			if (r.size() != 2)
			{
				logger.info("Failure in transactionalSuccess: Expected two rows received " + r.size());
				return;
			}
			if (!(r.get(0).equals(emp) && r.get(1).equals(emp2) || r.get(0).equals(emp2) && r.get(1).equals(emp)))
			{
				logger.info("Failure in transactionalSuccess: expected !=received, expected=" + emp + " and " + emp2
				        + ", received=" + r.get(0) + " and " + r.get(1));
			}
		}
		catch (QueryExecutionException e)
		{
			logger.info("Failure in transactionalSuccess: Exception while trying to get result from db");
			return;
		}
	}

	private static void transactionalFailure() throws PortKeyException
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		Employee emp = createEmployee();
		TransactionManager tm;
		try
		{
			tm = pl.getTransactionManager(emp, DataStoreType.RDBMS);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in transactionalFailure: Exception while trying to get transaction manager instance");
			return;
		}
		tm.begin();
		try
		{
			tm.insert(emp);
		}
		catch (QueryExecutionException e)
		{
			logger.info("Failure in transactionalFailure: Exception while trying to insert pojo");
			return;
		}
		Employee emp2 = createEmployee();
		try
		{
			tm.insert(emp2);
		}
		catch (Exception e)
		{
			logger.info("Failure in transactionalFailure: Exception while trying to insert pojo");
			return;
		}
		try
		{
			List<Employee> r = tm.getByCriteria(Employee.class, new HashMap<String, Object>());
			if (r.size() != 2)
			{
				logger.info("Failure in transactionalFailure: Expected two rows received " + r.size());
				return;
			}
			if (!(r.get(0).equals(emp) && r.get(1).equals(emp2) || r.get(0).equals(emp2) && r.get(1).equals(emp)))
			{
				logger.info("Failure in transactionalFailure: expected !=received, expected=" + emp + " and " + emp2
				        + ", received=" + r.get(0) + " and " + r.get(1));
			}
		}
		catch (QueryExecutionException e)
		{
			logger.info("Failure in transactionalFailure: Exception while trying to get result from db");
			return;
		}

		boolean exceptionEncountered = false;
		Employee emp3 = createEmployee();
		emp3.setPanCardNumber(RandomStringUtils.random(20, true, true));
		try
		{
			tm.insert(emp2);
		}
		catch (Exception e)
		{
			exceptionEncountered = true;
		}
		if (!exceptionEncountered)
		{
			logger.info("Failure in transactionalFailure: expected exception caught none");
			return;
		}
		tm.rollback();
		try
		{
			List<Employee> r = pl.getByCriteria(Employee.class, new HashMap<String, Object>());
			if (r != null && r.size() != 0)
			{
				logger.info("Failure in transactionalFailure: Expected zero rows received " + r.size());
				return;
			}
		}
		catch (QueryExecutionException e)
		{
			logger.info("Failure in transactionalFailure: Exception while trying to get result from db");
			return;
		}
	}

	private static Employee createEmployee()
	{
		Employee emp = new Employee();
		emp.setEmpId(RandomStringUtils.random(18, true, true));
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

	private static boolean cleanUp()
	{
		try
		{
			pl.updateBySql("portkey_example", "DELETE FROM employee", null);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while cleaning the table:" + e.toString());
			return false;
		}
		List<Employee> beans = getAllBeansFromTable();
		if (beans.size() != 0)
		{
			logger.info("Failed to clean tables, beans still present in table" + beans);
			return false;
		}
		return true;
	}

	private static List<Employee> getAllBeansFromTable()
	{
		List<Employee> l = null;
		try
		{
			l = pl.getBySql(Employee.class, "SELECT * FROM employee", null);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to retrieve all rows from table:" + e.toString());
		}
		return l;
	}

	private static void insert()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}

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
	}

	private static void insertWithGenerateId()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		Employee emp = createEmployee();
		try
		{
			Result r = pl.insert(emp, true);
			Employee returned = (Employee) r.getEntity();
			if (!returned.equals(emp))
			{
				logger.info("Failure in insertWithGenerateId: Bean returned by insert method does not match with passed one");
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
	}

	private static void upsert()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
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
	}

	private static void updateBean()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
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
	}

	private static void updateByCriteria()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
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
	}

	private static void delete()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
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
	}

	private static void getByCriteria()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
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
	}

	private static void getAllAttributesByCriteria()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
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
	}

	private static void getBeansBySql()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		Employee emp = insertAndReturnBean();
		List<Employee> l = null;
		try
		{
			l = pl.getBySql(Employee.class, "SELECT * FROM employee", null);
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

	}

	private static void getRsMapBySql()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		Employee emp = insertAndReturnBean();
		List<Row> tupleList = null;
		try
		{
			tupleList = pl.getBySql("portkey_example", "SELECT * FROM employee", null);
			if (tupleList == null || tupleList.size() == 0)
			{
				logger.info("Failure in getBeansBySql: No rows are returned by query");
				return;
			}
			else if (tupleList.size() == 1)
			{
				Row row = tupleList.get(0);
				Field[] fields = Employee.class.getFields();
				for (Field field : fields)
				{
					field.setAccessible(true);
					String fieldName = field.getName();
					if (!row.get(fieldName).equals(field.get(emp)))
					{
						logger.info("Failure in getRsMapBySql: Inconsistent values for field " + field.getName());
						logger.info("Expected:" + field.get(emp));
						logger.info("Received:" + row.get(fieldName));
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

	}

	private static UpdateQuery getUpdateQueryAndUpdateBean(Employee bean, boolean updateBean)
	{
		Map<String, Object> updateFieldNameToValueMap = new HashMap<String, Object>();
		String newName = RandomStringUtils.random(20, true, true);
		updateFieldNameToValueMap.put("name", newName);
		if (updateBean)
		{
			bean.setName(newName);
		}
		UpdateQuery q = new UpdateQuery();
		q.setClazz(bean.getClass());
		Map<String, Object> criteriaFieldNameToValueMap1 = new HashMap<String, Object>();
		criteriaFieldNameToValueMap1.put("empId", bean.getEmpId());
		q.setCriteriaFieldNameToValueMap(criteriaFieldNameToValueMap1);
		q.setUpdateFieldNameToValueMap(updateFieldNameToValueMap);
		return q;
	}

	private static UpdateQuery getUpdateQuery(Employee bean)
	{
		return getUpdateQueryAndUpdateBean(bean, false);
	}

	private static UpdateQuery getInvalidUpdateQuery(Employee bean)
	{
		Map<String, Object> updateFieldNameToValueMap = new HashMap<String, Object>();
		updateFieldNameToValueMap.put("panCardNumber", RandomStringUtils.random(20, true, true));
		UpdateQuery q = new UpdateQuery();
		q.setClazz(bean.getClass());
		Map<String, Object> criteriaFieldNameToValueMap1 = new HashMap<String, Object>();
		criteriaFieldNameToValueMap1.put("empId", bean.getEmpId());
		q.setCriteriaFieldNameToValueMap(criteriaFieldNameToValueMap1);
		q.setUpdateFieldNameToValueMap(updateFieldNameToValueMap);
		return q;
	}

	private static List<UpdateQuery> getQueries(List<Employee> beans, boolean updateBean)
	{
		List<UpdateQuery> queries = new ArrayList<UpdateQuery>();
		for (Employee bean : beans)
		{
			queries.add(getUpdateQueryAndUpdateBean(bean, updateBean));
		}
		return queries;
	}

	private static List<UpdateQuery> getQueriesInvalid(List<Employee> beans)
	{
		List<UpdateQuery> queries = new ArrayList<UpdateQuery>();
		int i = 0;
		for (Employee bean : beans)
		{
			i += 1;
			if (i == beans.size() - 1)
			{
				queries.add(getInvalidUpdateQuery(bean));
			}
			else
			{
				queries.add(getUpdateQuery(bean));
			}
		}
		return queries;
	}

	private static void updateTransactionalSuccess()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		List<Employee> expected = new ArrayList<Employee>();
		for (int i = 0; i < 4; i++)
		{
			expected.add(insertAndReturnBean());
		}
		List<UpdateQuery> queries = getQueries(expected, true);
		Result result = null;
		try
		{
			result = pl.update(queries);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in updateTransactional: caught exception", e);
			return;
		}
		int rowsUpdated = result.getRowsUpdatedForDataStore(DataStoreType.RDBMS);
		if (rowsUpdated != queries.size())
		{
			logger.info("Failure in updateTransactional: number of update queries sent =" + queries.size()
			        + ", number of rows updated=" + rowsUpdated);
			return;
		}
		List<Employee> actual = getAllBeansFromTable();
		if (expected.size() != actual.size())
		{
			logger.info("Failure in updateTransactional: expected=" + expected + ", actual=" + actual);
		}
		Set<Employee> expectedSet = new HashSet<Employee>(expected);
		Set<Employee> actualSet = new HashSet<Employee>(actual);

		if (!expectedSet.equals(actualSet))
		{
			logger.info("Failure in updateTransactional: expected=" + expected + ", actual=" + actual);
		}

	}

	private static void updateTransactionalSuccessWithNoRowsUpdate()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		List<Employee> expected = new ArrayList<Employee>();
		for (int i = 0; i < 4; i++)
		{
			expected.add(insertAndReturnBean());
		}
		List<UpdateQuery> queries = getQueries(expected, true);
		Result result = null;
		try
		{
			result = pl.update(queries, true);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in updateTransactional: caught exception", e);
			return;
		}
		int rowsUpdated = result.getRowsUpdatedForDataStore(DataStoreType.RDBMS);
		if (rowsUpdated != queries.size())
		{
			logger.info("Failure in updateTransactional: number of update queries sent =" + queries.size()
			        + ", number of rows updated=" + rowsUpdated);
			return;
		}
		List<Employee> actual = getAllBeansFromTable();
		if (expected.size() != actual.size())
		{
			logger.info("Failure in updateTransactional: expected=" + expected + ", actual=" + actual);
		}
		Set<Employee> expectedSet = new HashSet<Employee>(expected);
		Set<Employee> actualSet = new HashSet<Employee>(actual);

		if (!expectedSet.equals(actualSet))
		{
			logger.info("Failure in updateTransactional: expected=" + expected + ", actual=" + actual);
		}
	}

	private static void updateTransactionalFailureWithNoRowsUpdate()
	{
		boolean exceptionEncountered = false;
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		List<Employee> expected = new ArrayList<Employee>();
		for (int i = 0; i < 4; i++)
		{
			expected.add(insertAndReturnBean());
		}
		List<UpdateQuery> queries = getQueries(expected, false);
		queries.get(2).getCriteriaFieldNameToValueMap().put("name", "someNameWhichIsNotPresentInTable");
		try
		{
			pl.update(queries, true);
		}
		catch (PortKeyException e)
		{
			exceptionEncountered = true;
		}
		if (!exceptionEncountered)
		{
			logger.info("Failure in updateTransactionalFailureWithNoRowsUpdate, exception was expected but encountered none");
			return;
		}
		List<Employee> actual = getAllBeansFromTable();
		if (expected.size() != actual.size())
		{
			logger.info("Failure in updateTransactional: expected=" + expected + ", actual=" + actual);
		}
		Set<Employee> expectedSet = new HashSet<Employee>(expected);
		Set<Employee> actualSet = new HashSet<Employee>(actual);

		if (!expectedSet.equals(actualSet))
		{
			logger.info("Failure in updateTransactional: expected=" + expected + ", actual=" + actual);
		}
	}

	private static void updateTransactionalFailure()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		boolean exceptionCaught = false;
		List<Employee> expected = new ArrayList<Employee>();
		for (int i = 0; i < 4; i++)
		{
			expected.add(insertAndReturnBean());
		}
		List<UpdateQuery> queries = getQueriesInvalid(expected);
		try
		{
			pl.update(queries);
		}
		catch (PortKeyException e)
		{
			exceptionCaught = true;
		}
		if (!exceptionCaught)
		{
			logger.info("Failure in updateTransactionalFailure: exception was expected, but didn't catch any");
			return;
		}

		List<Employee> actual = getAllBeansFromTable();
		if (expected.size() != actual.size())
		{
			logger.info("Failure in updateTransactionalFailure: expected=" + expected + ", actual=" + actual);
		}
		Set<Employee> expectedSet = new HashSet<Employee>(expected);
		Set<Employee> actualSet = new HashSet<Employee>(actual);

		if (!expectedSet.equals(actualSet))
		{
			logger.info("Failure in updateTransactionalFailure: expected=" + expected + ", actual=" + actual);
		}
	}

	private static boolean compareLists(List<Employee> expected, List<Employee> actual)
	{
		if (expected.size() != actual.size())
		{
			return false;
		}
		Set<Employee> expectedSet = new HashSet<Employee>(expected);
		Set<Employee> actualSet = new HashSet<Employee>(actual);

		if (expectedSet.equals(actualSet))
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	private static void insertTransactionalSuccess()
	{
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		List<Employee> expected = new ArrayList<Employee>();
		for (int i = 0; i < 4; i++)
		{
			expected.add(createEmployee());
		}
		try
		{
			pl.insert(expected);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in insertTransactionalSuccess: Exception while trying to insert beans" + e);
			return;
		}
		List<Employee> actual = getAllBeansFromTable();
		if (!compareLists(expected, actual))
		{
			logger.info("Failure, expected=" + expected + ", actual=" + actual);
		}
	}

	private static void insertTransactionalFailure()
	{
		boolean exceptionEncountered = false;
		if (!cleanUp())
		{
			logger.info("Failed to clean up tables");
			return;
		}
		List<Employee> expected = new ArrayList<Employee>();
		for (int i = 0; i < 4; i++)
		{
			Employee bean = createEmployee();
			if (i == 2)
			{
				bean.setPanCardNumber(RandomStringUtils.random(20, true, true));
			}
			expected.add(bean);
		}
		try
		{
			pl.insert(expected);
		}
		catch (PortKeyException e)
		{
			exceptionEncountered = true;
		}
		if (!exceptionEncountered)
		{
			logger.info("Failure: Exception was expected but didn't catch any");
			return;
		}
		List<Employee> actual = getAllBeansFromTable();
		if (actual != null && actual.size() != 0)
		{
			logger.info("Failure, expected an empty set, actual=" + actual);
		}
	}

	private static String generateRandomShardId()
	{
		String[] shardIds = {"01", "02"};
		return shardIds[new Random().nextInt(shardIds.length)];
	}

	private static EmployeeSharded createEmployee2()
	{
		EmployeeSharded emp = new EmployeeSharded();
		emp.setEmpId(RandomStringUtils.random(16, true, true) + generateRandomShardId());
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

	private static EmployeeSharded insertAndReturnBean2()
	{
		EmployeeSharded emp = createEmployee2();
		Result result = null;
		try
		{
			result = pl.insert(emp, true);
			emp = (EmployeeSharded) result.getEntity();
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in insert: Exception while trying to insert bean" + e);
		}
		return emp;
	}

	private static boolean cleanUp2()
	{
		try
		{
			pl.updateBySql("portkey_example_sharded", "DELETE FROM employee", null);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while cleaning the table:" + e.toString());
		}
		List<Employee> beans = getAllBeansFromTable();
		if (beans.size() != 0)
		{
			logger.info("Failed to clean tables, beans still present in table" + beans);
			return false;
		}
		return true;
	}

	private static List<EmployeeSharded> getAllBeansFromTable2()
	{
		List<EmployeeSharded> l = null;
		try
		{
			l = pl.getBySql(EmployeeSharded.class, "SELECT * FROM employee", null);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to retrieve all rows from table:" + e.toString());
		}
		return l;
	}

	private static void insert2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}

		EmployeeSharded emp = createEmployee2();

		try
		{
			Result r = pl.insert(emp);
			EmployeeSharded returned = (EmployeeSharded) r.getEntity();
			if (!returned.equals(emp))
			{
				logger.info("Failure in insert2: Bean returned by insert method does not match with passed one");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in insert2: Exception while trying to insert bean" + e);
			return;
		}
		List<EmployeeSharded> l = getAllBeansFromTable2();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in insert2: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			EmployeeSharded returned = l.get(0);
			if (!emp.equals(returned))
			{
				logger.info("Failure in insert2: Inserted bean doesn't match with fetched bean");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		else
		{
			logger.info("Failure in insert2: Inserted only one bean but query returned more");
			return;
		}

	}

	private static void insertWithGenerateId2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = createEmployee2();
		try
		{
			Result r = pl.insert(emp, true);
			EmployeeSharded returned = (EmployeeSharded) r.getEntity();
			if (!returned.equals(emp))
			{
				logger.info("Failure in insertWithGenerateId2: Bean returned by insert method does not match with passed one");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in insertWithGenerateId2: Exception while trying to insert bean" + e);
			return;
		}
		List<EmployeeSharded> l = getAllBeansFromTable2();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in insertWithGenerateId2: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			EmployeeSharded returned = l.get(0);
			if (!emp.equals(l.get(0)))
			{
				logger.info("Failure in insertWithGenerateId2: bean doesn't match with fetched bean");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		else
		{
			logger.info("Failure in insertWithGenerateId2: Inserted only one bean but query returned more");
			return;
		}

	}

	private static void upsert2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = createEmployee2();
		try
		{
			Result r = pl.upsert(emp);
			EmployeeSharded returned = (EmployeeSharded) r.getEntity();
			if (!returned.equals(emp))
			{
				logger.info("Failure in upser2t: Bean returned by upsert method does not match with passed one");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in upsert2: Exception while trying to upsert bean" + e);
			return;
		}
		List<EmployeeSharded> l = getAllBeansFromTable2();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in upsert2: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				logger.info("Failure in upsert2: bean doesn't match with fetched bean");
				return;
			}
		}
		else
		{
			logger.info("Failure in upsert2: Inserted only one bean but query returned more");
			return;
		}

	}

	private static void updateBean2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = insertAndReturnBean2();
		emp.setName(emp.getName() + " Some Surname");
		try
		{
			Result r = pl.update(emp);
			EmployeeSharded returned = (EmployeeSharded) r.getEntity();
			if (!returned.equals(emp))
			{
				logger.info("Failure in updateBean2: Bean returned by insert method does not match with passed one");
				logger.info("Passed:" + emp);
				logger.info("Returned:" + returned);
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in updateBean2: Exception while trying to update bean" + e);
			return;
		}
		List<EmployeeSharded> l = getAllBeansFromTable2();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in updateBean2: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				logger.info("Failure in updateBean2: Inserted bean doesn't match with fetched bean");
				return;
			}
		}
		else
		{
			logger.info("Failure in updateBean2: Inserted only one bean but query returned more");
			return;
		}

	}

	private static void updateByCriteria2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = insertAndReturnBean2();
		try
		{
			Map<String, Object> updateValuesMap = new HashMap<String, Object>();
			List<String> pastEmployers = emp.getPastEmployers();
			pastEmployers.add("University of Washington");
			emp.setPastEmployers(pastEmployers);
			updateValuesMap.put("pastEmployers", pastEmployers);
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("panCardNumber", emp.getPanCardNumber());
			pl.update(EmployeeSharded.class, updateValuesMap, criteria);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in updateByCriteria2: Exception while trying to update bean:" + e);
			return;
		}
		List<EmployeeSharded> l = getAllBeansFromTable2();
		if (l == null || l.size() == 0)
		{
			logger.info("Failure in updateByCriteria2: No rows are returned by query");
			return;
		}
		else if (l.size() == 1)
		{
			if (!emp.equals(l.get(0)))
			{
				logger.info("Failure in updateByCriteria2: Inserted bean doesn't match with fetched bean");
				logger.info("Inserted:" + emp);
				logger.info("Returned:" + l.get(0));
				return;
			}
		}
		else
		{
			logger.info("Failure in updateByCriteria2: Inserted only one bean but query returned more");
			return;
		}

	}

	private static void delete2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = insertAndReturnBean2();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("gender", emp.getGender());
			pl.delete(EmployeeSharded.class, criteria);
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in delete2: Exception while trying to delete bean:" + e);
			return;
		}
		List<EmployeeSharded> l = getAllBeansFromTable2();
		if (l != null && l.size() != 0)
		{
			logger.info("Failure in delete2: Non zero number of rows are returned by query");
			return;
		}

	}

	private static void getByCriteria2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = insertAndReturnBean2();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("address", emp.getAddress());
			List<String> attributes = new ArrayList<String>();
			attributes.add("name");
			attributes.add("joiningDate");
			attributes.add("annualSalary");
			EmployeeSharded expected = new EmployeeSharded();
			expected.setName(emp.getName());
			expected.setAnnualSalary(emp.getAnnualSalary());
			expected.setJoiningDate(emp.getJoiningDate());
			List<EmployeeSharded> l = pl.getByCriteria(EmployeeSharded.class, attributes, criteria);
			if (l == null || l.size() == 0)
			{
				logger.info("Failure in getByCriteria2: No rows are returned by query");
				return;
			}
			else if (l.size() == 1)
			{
				if (!expected.equals(l.get(0)))
				{
					logger.info("Failure in getByCriteria2: Inserted bean doesn't match with fetched bean");
					logger.info("Expected:" + emp);
					logger.info("Returned:" + l.get(0));
					return;
				}
			}
			else
			{
				logger.info("Failure in getByCriteria2: Inserted only one bean but query returned more");
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in getByCriteria2: Exception while trying to delete bean:" + e);
			return;
		}
	}

	private static void getAllAttributesByCriteria2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = insertAndReturnBean2();
		try
		{
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("address", emp.getAddress());
			List<EmployeeSharded> l = pl.getByCriteria(EmployeeSharded.class, criteria);
			if (l == null || l.size() == 0)
			{
				logger.info("Failure in getAllAttributesByCriteria2: No rows are returned by query");
				return;
			}
			else if (l.size() == 1)
			{
				if (!emp.equals(l.get(0)))
				{
					System.out
					        .println("Failure in getAllAttributesByCriteria2: Inserted bean doesn't match with fetched bean");
					logger.info("Inserted:" + emp);
					logger.info("Returned:" + l.get(0));
					return;
				}
			}
			else
			{
				logger.info("Failure in getAllAttributesByCriteria2: Inserted only one bean but query returned more");
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Failure in getAllAttributesByCriteria2: Exception while trying to delete bean:" + e);
			return;
		}
	}

	private static void getBeansBySql2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = insertAndReturnBean2();
		List<EmployeeSharded> l = null;
		try
		{
			l = pl.getBySql(EmployeeSharded.class, "SELECT * FROM employee", null);
			if (l == null || l.size() == 0)
			{
				logger.info("Failure in getBeansBySql2: No rows are returned by query");
				return;
			}
			else if (l.size() == 1)
			{
				if (!emp.equals(l.get(0)))
				{
					logger.info("Failure in getBeansBySql2: Inserted bean doesn't match with fetched bean");
					logger.info("Inserted:" + emp);
					logger.info("Returned:" + l.get(0));
					return;
				}
			}
			else
			{
				logger.info("Failure in getBeansBySql2: Inserted only one bean but query returned more");
				return;
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to retrieve all rows from table:" + e.toString());
			return;
		}
	}

	private static void getRsMapBySql2()
	{
		if (!cleanUp2())
		{
			logger.info("Failed to clean up tables");
		}
		EmployeeSharded emp = insertAndReturnBean2();
		List<Row> rs = null;
		try
		{
			rs = pl.getBySql("portkey_example_sharded", "SELECT * FROM employee", null);
			if (rs == null || rs.size() == 0)
			{
				logger.info("Failure in getBeansBySql2: No rows are returned by query");
				return;
			}
			else if (rs.size() == 1)
			{
				Row row = rs.get(0);
				Field[] fields = EmployeeSharded.class.getFields();
				for (Field field : fields)
				{
					field.setAccessible(true);
					String fieldName = field.getName();
					if (!row.get(fieldName).equals(field.get(emp)))
					{
						logger.info("Failure in getRsMapBySql2: Inconsistent values for field " + field.getName());
						logger.info("Expected:" + field.get(emp));
						logger.info("Received:" + row.get(fieldName));
						return;
					}
				}
			}
			else
			{
				logger.info("Failure in getBeansBySql2: Inserted only one bean but query returned more");
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
	}
}
