/**
 * 
 */
package com.flipkart.portkey.example;

import java.util.ArrayList;
import java.util.Date;
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
import com.flipkart.portkey.example.dao.Address;
import com.flipkart.portkey.example.dao.Gender;
import com.flipkart.portkey.example.dao.Person;
import com.flipkart.portkey.persistence.PersistenceLayer;

/**
 * @author santosh.p
 */
public class Example
{
	private static Logger logger = Logger.getLogger(Example.class);
	private static PersistenceLayer pl;
	private static Person person = createPerson();

	private static void insert()
	{
		try
		{
			Result r = pl.insert(person);
			person = (Person) r.getEntity();
			logger.info("Successfully inserted the bean, inserted bean is" + person);
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
			Result r = pl.insert(person, true);
			person = (Person) r.getEntity();
			logger.info("Successfully inserted the bean, inserted bean is" + person);
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
			List<Person> rs = pl.getByCriteria(Person.class, getPkCriteria(person));
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Person p : rs)
				{
					logger.info(p);
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
			List<Person> rs = pl.getByCriteria(Person.class, getNonPkCriteria(person));
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Person p : rs)
				{
					logger.info(p);
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
			person.setName("SomeOtherName");
			Result result = pl.update(person);
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
			logger.info("Exception while trying to update bean");
		}
	}

	private static void delete()
	{
		try
		{
			Result result = pl.delete(Person.class, getPkCriteria(person));
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
			String sql = "SELECT * FROM Person WHERE id=:id";
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("id", "PRSN0293801");
			List<Person> rs = pl.getBySql(Person.class, sql, criteria);
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Person p : rs)
				{
					logger.info(p);
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
			String sql = "SELECT * FROM Person WHERE gender=:gender";
			Map<String, Object> criteria = new HashMap<String, Object>();
			criteria.put("gender", "MALE");
			List<Person> rs = pl.getBySql(Person.class, sql, criteria);
			if (rs == null)
			{
				logger.info("received null resultset");
			}
			else
			{
				logger.info("Successfully fetched data!!");
				for (Person p : rs)
				{
					logger.info(p);
				}
			}
		}
		catch (PortKeyException e)
		{
			logger.info("Exception while trying to fetch bean" + e);
		}
	}

	public static void main(String[] args)
	{
		Scanner sc = new Scanner(System.in);
		ApplicationContext context =
		        new FileSystemXmlApplicationContext("src/main/resources/external/portkey-application-context.xml");
		pl = context.getBean(PersistenceLayer.class, "persistenceLayer");
		insert();
		sc.next();
		// generateAndInsert();
		// sc.next();
		getByCriteriaUsingPk();
		// sc.next();
		getByCriteriaUsingNonPk();
		sc.next();
		// updateByCriteria();
		// sc.next();
		// sc.next();
		getBySqlUsingPk();
		getBySqlUsingNonPk();
		sc.close();
	}

	/**
	 * @param person
	 * @return
	 */
	private static Map<String, Object> getPkCriteria(Person person)
	{
		Map<String, Object> criteriaMap = new HashMap<String, Object>();
		criteriaMap.put("id", person.getId());
		return criteriaMap;
	}

	/**
	 * @param person
	 * @return
	 */
	private static Map<String, Object> getNonPkCriteria(Person person)
	{
		Map<String, Object> criteriaMap = new HashMap<String, Object>();
		criteriaMap.put("name", person.getName());
		return criteriaMap;
	}

	/**
	 * @return
	 */
	private static Person createPerson()
	{
		Person p = new Person();
		p.setName("someName");
		p.setGender(Gender.MALE);
		p.setId("PRSN0293801");
		List<String> placesVisited = new ArrayList<String>();
		placesVisited.add("Delhi");
		placesVisited.add("Kolkata");
		placesVisited.add("Hyderabad");
		p.setPlacesVisited(placesVisited);
		Address a = new Address();
		a.setLine1("Sakra Hospital");
		a.setLine2("Near JP Morgan");
		a.setArea("Kormangala");
		a.setCity("Chennai");
		a.setPinCode(400076);
		p.setAddress(a);
		p.setAnnualIncome(1000000);
		p.setLastLogin(new Date());
		return p;
	}
}
