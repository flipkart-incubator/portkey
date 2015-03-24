package com.flipkart.portkey.common.util;

import java.lang.reflect.Field;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import com.flipkart.portkey.common.entity.Entity;

public class PortKeyUtilTest
{
	private class Person implements Entity
	{
		private String firstName;
		private String lastName;

		public String toString()
		{
			return firstName == null ? "" : firstName + lastName == null ? "" : lastName;
		}
	}

	@Test
	public void testGetFieldFromBean()
	{
		Person p = new Person();
		p.firstName = "SomeName";
		p.lastName = "SomeLastName";
		Field expected = null;
		try
		{
			expected = p.getClass().getField("firstName");
		}
		catch (SecurityException e)
		{
			Assert.fail("Failed to get field from bean");
		}
		catch (NoSuchFieldException e)
		{
			Assert.fail("Failed to get field from bean");
		}
		Field actual = PortKeyUtils.getFieldFromBean(p, "firstName");
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetFieldValueFromBean()
	{
		Person p = new Person();
		p.firstName = "SomeName";
		p.lastName = "SomeLastName";
		Object expected = null;
		try
		{
			Field field = p.getClass().getField("firstName");
			field.setAccessible(true);
			expected = field.get(p);
		}
		catch (SecurityException e)
		{
			Assert.fail("Failed to get value from bean");
		}
		catch (NoSuchFieldException e)
		{
			Assert.fail("Failed to get value from bean");
		}
		catch (IllegalArgumentException e)
		{
			Assert.fail("Failed to get value from bean");
		}
		catch (IllegalAccessException e)
		{
			Assert.fail("Failed to get value from bean");
		}
		Object actual = PortKeyUtils.getFieldValueFromBean(p, "firstName");
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testSetFieldValueInBean()
	{
		Person p = new Person();
		String expected = "ExpectedLastName";
		PortKeyUtils.setFieldValueInBean(p, "lastName", expected);
		String actual = p.lastName;
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testToString() throws Exception
	{
		Assert.assertEquals(PortKeyUtils.toString(null), null);
		Person p = new Person();
		p.firstName = "SomeName";
		p.lastName = "SomeLastName";
		Assert.assertEquals(PortKeyUtils.toString(p), p.toString());
	}

	@Test
	@Ignore
	public void testToJsonString()
	{
		// TODO:implement
	}
}
