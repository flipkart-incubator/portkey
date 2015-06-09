package com.flipkart.portkey.rdbms.querybuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.portkey.rdbms.dao.Person;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.metadata.RdbmsTableMetaData;

public class RdbmsQueryBuilderTest
{
	Logger logger = LoggerFactory.getLogger(RdbmsQueryBuilderTest.class);

	@Test
	public void testGetInsertQuery()
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(Person.class);
		String actual = RdbmsQueryBuilder.getInstance().getInsertQuery(metaData);
		String expected =
		        "INSERT INTO person" + "\n (`id`,`first_name`,`last_name`,`age`,`mod_count`,`last_modified`)"
		                + "\nVALUES (:id,:first_name,:last_name,:age,:mod_count,now())";

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetUpsertQuery()
	{
		String expected =
		        "INSERT INTO person"
		                + "\n (`id`,`first_name`,`last_name`,`age`,`mod_count`,`last_modified`)"
		                + "\nVALUES (:id,:first_name,:last_name,:age,:mod_count,now())"
		                + "\nON DUPLICATE KEY UPDATE `first_name`=:first_name,`last_name`=:last_name,`age`=:age,`mod_count`=mod_count+1,`last_modified`=now()";
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(Person.class);
		String actual = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetUpsertQueryWithSpecificColumnsToBeUpdated()
	{
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(Person.class);
		List<String> fieldsToBeUpdatedOnDuplicate = new ArrayList<String>();
		fieldsToBeUpdatedOnDuplicate.add("firstName");
		fieldsToBeUpdatedOnDuplicate.add("lastName");
		fieldsToBeUpdatedOnDuplicate.add("modCount");
		fieldsToBeUpdatedOnDuplicate.add("lastModified");
		String expected =
		        "INSERT INTO person"
		                + "\n (`id`,`first_name`,`last_name`,`age`,`mod_count`,`last_modified`)"
		                + "\nVALUES (:id,:first_name,:last_name,:age,:mod_count,now())"
		                + "\nON DUPLICATE KEY UPDATE `first_name`=:first_name,`last_name`=:last_name,`mod_count`=mod_count+1,`last_modified`=now()";
		String actual = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData, fieldsToBeUpdatedOnDuplicate);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetUpdateByPkQuery()
	{
		String expected =
		        "INSERT INTO person"
		                + "\n (`id`,`first_name`,`last_name`,`age`,`mod_count`,`last_modified`)"
		                + "\nVALUES (:id,:first_name,:last_name,:age,:mod_count,now())"
		                + "\nON DUPLICATE KEY UPDATE `first_name`=:first_name,`last_name`=:last_name,`age`=:age,`mod_count`=mod_count+1,`last_modified`=now()";
		RdbmsTableMetaData metaData = RdbmsMetaDataCache.getInstance().getMetaData(Person.class);
		String actual = RdbmsQueryBuilder.getInstance().getUpsertQuery(metaData);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetUpdateByCriteriaQuery()
	{
		String expected =
		        "UPDATE person"
		                + "\nSET `firstName`=:firstName_update, `lastName`=:lastName_update, `someAttribute`=:someAttribute_update, `mod_count`=mod_count+1"
		                + "\nWHERE (`id`=:id_criteria AND `someAttribute`=:someAttribute_criteria)";
		String tableName = "person";
		Map<String, Object> updateColumnToValueMap = new LinkedHashMap<String, Object>();
		updateColumnToValueMap.put("firstName", "someFirstName");
		updateColumnToValueMap.put("lastName", "someLastName");
		updateColumnToValueMap.put("someAttribute", "newValue");
		RdbmsSpecialValue modCount = new RdbmsSpecialValue();
		modCount.setValue("mod_count+1");
		updateColumnToValueMap.put("mod_count", modCount);
		Map<String, Object> criteriaColumnToValueMap = new LinkedHashMap<String, Object>();
		criteriaColumnToValueMap.put("id", "someId");
		criteriaColumnToValueMap.put("someAttribute", "oldValue");
		Map<String, Object> placeHolderToValueMap = new LinkedHashMap<String, Object>();
		String actual =
		        RdbmsQueryBuilder.getInstance().getUpdateByCriteriaQuery(tableName, updateColumnToValueMap,
		                criteriaColumnToValueMap, placeHolderToValueMap);
		Assert.assertEquals(expected, actual);
	}

	@Test
	@Ignore
	public void testGetDeleteByCriteriaQuery()
	{

	}
}
