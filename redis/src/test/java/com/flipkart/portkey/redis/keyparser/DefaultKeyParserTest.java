/**
 * 
 */
package com.flipkart.portkey.redis.keyparser;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.redis.metadata.RedisMetaData;
import com.flipkart.portkey.redis.metadata.RedisMetaDataCache;

/**
 * @author santosh.p
 */
@RunWith (PowerMockRunner.class)
@PrepareForTest (DefaultKeyParser.class)
public class DefaultKeyParserTest
{
	private RedisMetaData getRedisMetaData(Class<? extends Entity> clazz)
	{
		RedisMetaDataCache metaDataCache = RedisMetaDataCache.getInstance();
		return metaDataCache.getMetaData(clazz);

	}

	@Test
	public void testParsePrimaryKeyPattern()
	{
		DefaultKeyParser keyParser = new DefaultKeyParser();
		Person person = new Person();
		person.setId("PN2948I");
		person.setName("SomeName");
		RedisMetaData metaData = getRedisMetaData(Person.class);
		Assert.assertEquals("PersonID:PN2948I+NAME:SomeName", keyParser.parsePrimaryKeyPattern(person, metaData).get(0));
	}
}
