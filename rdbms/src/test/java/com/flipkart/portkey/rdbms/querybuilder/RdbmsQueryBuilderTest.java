package com.flipkart.portkey.rdbms.querybuilder;

import java.util.ArrayList;

import org.junit.Ignore;
import org.junit.Test;

import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;

public class RdbmsQueryBuilderTest
{
	@Test
	public void testGetInsertQuery()
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(Person.class);
		String actual = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		String expected =
		        "INSERT INTO person" + "\n (`id`,`last_name`,`age`,`first_name`)"
		                + "\nVALUES (:id,:last_name,:age,:first_name)";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetUpsertQuery()
	{
		String expected =
		        "INSERT INTO person" + "\n (`id`,`last_name`,`age`,`first_name`)"
		                + "\nVALUES (:id,:last_name,:age,:first_name)"
		                + "\nON DUPLICATE KEY UPDATE `last_name`=:last_name,`age`=:age,`first_name`=:first_name";
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(Person.class);
		String actual = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetUpsertQueryWithSpecificColumnsToBeUpdated()
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(Person.class);
		List<String> fieldsToBeUpdatedOnDuplicate = new ArrayList<String>();
		fieldsToBeUpdatedOnDuplicate.add("lastName");
		fieldsToBeUpdatedOnDuplicate.add("firstName");
		String expected =
		        "INSERT INTO person" + "\n (`id`,`last_name`,`age`,`first_name`)"
		                + "\nVALUES (:id,:last_name,:age,:first_name)"
		                + "\nON DUPLICATE KEY UPDATE `last_name`=:last_name,`first_name`=:first_name";
		String actual = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData, fieldsToBeUpdatedOnDuplicate);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetUpdateByPkQuery()
	{
		String expected =
		        "INSERT INTO person" + "\n (`id`,`last_name`,`age`,`first_name`)"
		                + "\nVALUES (:id,:last_name,:age,:first_name)"
		                + "\nON DUPLICATE KEY UPDATE `last_name`=:last_name,`age`=:age,`first_name`=:first_name";
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(Person.class);
		String actual = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Assert.assertEquals(expected, actual);
	}

	@Test
	@Ignore
	public void testGetUpdateByCriteriaQuery()
	{

	}

	@Test
	@Ignore
	public void testGetDeleteByCriteriaQuery()
	{

	}

	@Test
	public void testGetByJoinCriteriaQuery()
	{
		RdbmsJoinMetaData metaData = RdbmsMetaDataCache.getInstance().getJoinMetaData(JoinForTest.class);
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add("t1c1");
		fieldNames.add("t2c1");
		fieldNames.add("t3c1");
		fieldNames.add("t4c1");

		List<String> criteriaAttributes = new ArrayList<String>();
		criteriaAttributes.add("t1c2");
		criteriaAttributes.add("t2c2");
		criteriaAttributes.add("t3c2");
		criteriaAttributes.add("t4c2");
		String expected =
		        "SELECT t1.t1_c1, t2.t2_c1, t3.t3_c1, t4.t4_c1"
		                + "\nFROM table1 AS t1 INNER JOIN table2 AS t2 ON t1.t1c1=t2.t2c2 OUTER JOIN table3 AS t3 ON t2.t2c1<t3.t3c2 EQUI JOIN table4 AS t4 ON t3.t3c1>t4.t4c2"
		                + "\nWHERE `t1.t1c2`=:t1c2,`t2.t2c2`=:t2c2,`t3.t3c2`=:t3c2,`t4.t4c2`=:t4c";
		String actual =
		        RdbmsQueryBuilder.getInstance().getGetByJoinCriteriaQuery(metaData, fieldNames, criteriaAttributes);
		Assert.assertEquals(expected, actual);
	}
}
