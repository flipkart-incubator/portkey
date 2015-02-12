/**
 * 
 */
package com.flipkart.portkey.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.flipkart.portkey.common.enumeration.DataStoreType;
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
	private static PersistenceLayer pl;
	private static Employee employee = createEmployee();

	public static void main(String[] args)
	{
		Scanner sc = new Scanner(System.in);
		ApplicationContext context =
		        new FileSystemXmlApplicationContext("src/main/resources/external/portkey-application-context.xml");
		pl = context.getBean(PersistenceLayer.class, "persistenceLayer");
		// insert();
		// sc.next();
		// getByCriteriaUsingPk();
		// sc.next();
		// getByCriteriaUsingNonPk();
		// sc.next();
		// getByCriteriaUsingSecondaryKey();
		// sc.next();
		// updateByCriteria();
		// sc.next();
		// getBySqlUsingPk();
		// sc.next();
		// getBySqlUsingNonPk();
		// sc.next();
		upsert();
		sc.close();
	}

	private static Map<String, Object> getPkCriteria(Employee employee)
	{
		Map<String, Object> criteriaMap = new HashMap<String, Object>();
		criteriaMap.put("empId", employee.getEmpId());
		criteriaMap.put("aadharCardNumber", employee.getAadharCardNumber());
		return criteriaMap;
	}

	private static Map<String, Object> getSecondaryKeyCriteria(Employee employee)
	{
		Map<String, Object> criteriaMap = new HashMap<String, Object>();
		criteriaMap.put("panCardNumber", employee.getPanCardNumber());
		return criteriaMap;
	}

	private static Map<String, Object> getNonPkCriteria(Employee employee)
	{
		Map<String, Object> criteriaMap = new HashMap<String, Object>();
		criteriaMap.put("annualSalary", employee.getAnnualSalary());
		return criteriaMap;
	}

	/**
	 * @return
	 */
	private static Employee createEmployee()
	{
		Employee emp = new Employee();
		emp.setEmpId("EE74859601");
		emp.setName("Some Name");
		// emp.setDob(new Date());
		emp.setAnnualSalary(1234560);
		emp.setPanCardNumber("ABBPP738P");
		emp.setAadharCardNumber("3948ADH38A");
		emp.setGender(Gender.MALE);
		emp.setAddress(null);
		ArrayList<String> pastEmployers = new ArrayList<String>();
		pastEmployers.add("BlackStone");
		pastEmployers.add("Quora");
		pastEmployers.add("GOI");
		emp.setPastEmployers(pastEmployers);
		return emp;

	}

	private static void insert()
	{
		try
		{
			Result r = pl.insert(employee);
			employee = (Employee) r.getEntity();
			logger.info("Successfully inserted the bean, inserted bean is" + employee);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to insert bean" + e);
		}
	}

	private static void upsert()
	{
		try
		{
			employee.setName("UpsertName");
			Result r = pl.upsert(employee);
			employee = (Employee) r.getEntity();
			logger.info("Successfully inserted the bean, inserted bean is" + employee);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to insert bean" + e);
		}
	}

	private static void generateAndInsert()
	{
		try
		{
			Result r = pl.insert(employee, true);
			employee = (Employee) r.getEntity();
			logger.info("Successfully inserted the bean, inserted bean is" + employee);
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to insert bean" + e);
		}
	}

	private static void getByCriteriaUsingPk()
	{
		try
		{
			List<Employee> rs = pl.getByCriteria(Employee.class, getPkCriteria(employee));
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Employee e : rs)
				{
					logger.info(e);
				}
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to fetch bean" + e);
		}
	}

	private static void getByCriteriaUsingSecondaryKey()
	{
		try
		{
			List<Employee> rs = pl.getByCriteria(Employee.class, getSecondaryKeyCriteria(employee));
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Employee e : rs)
				{
					logger.info(e);
				}
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to fetch bean" + e);
		}
	}

	private static void getByCriteriaUsingNonPk()
	{
		try
		{
			List<Employee> rs = pl.getByCriteria(Employee.class, getNonPkCriteria(employee));
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Employee e : rs)
				{
					logger.info(e);
				}
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to fetch bean" + e);
		}
	}

	private static void updateByCriteria()
	{
		try
		{
			employee.setName("SomeOtherName");
			Result result = pl.update(employee);
			if (result.getRowsUpdatedForDataStore(DataStoreType.REDIS) > 0)
				logger.info("Successfully updated bean in redis");
			else
				logger.info("Failed to update in redis");
			if (result.getRowsUpdatedForDataStore(DataStoreType.RDBMS) > 0)
				logger.info("Successfully updated bean in rdbms");
			else
				logger.info("Failed to update in rdbms");
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to update bean", e);
		}
	}

	private static void delete()
	{
		try
		{
			Result result = pl.delete(Employee.class, getPkCriteria(employee));
			if (result.getRowsUpdatedForDataStore(DataStoreType.REDIS) > 0)
				logger.info("Successfully deleted bean from redis");
			else
				logger.info("Failed to delete from redis");
			if (result.getRowsUpdatedForDataStore(DataStoreType.RDBMS) > 0)
				logger.info("Successfully deleted bean from rdbms");
			else
				logger.info("Failed to delete from rdbms");
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while deleting bean");
		}
	}

	private static void getBySqlUsingPk()
	{
		try
		{
			String sql = "SELECT * FROM Employee WHERE emp_id=:empId";
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("empId", employee.getEmpId());
			List<Employee> rs = pl.getBySql(Employee.class, sql, criteria);
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Employee emp : rs)
				{
					logger.info(emp);
				}
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to fetch bean" + e);
		}
	}

	private static void getBySqlUsingNonPk()
	{
		try
		{
			String sql = "SELECT * FROM Employee WHERE gender=:gender";
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("gender", "MALE");
			List<Employee> rs = pl.getBySql(Employee.class, sql, criteria);
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Employee emp : rs)
				{
					logger.info(emp);
				}
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to fetch bean" + e);
		}
	}
}
