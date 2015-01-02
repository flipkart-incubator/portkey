/**
 * 
 */
package com.flipkart.portkey.redis.keyparser;

import java.io.Serializable;

/**
 * @author santosh.p
 */
public class Address implements Serializable
{

	private static final long serialVersionUID = -5234877109870705438L;

	private String line1;
	private String line2;
	private String area;
	private String city;
	private int pinCode;

	public String getLine1()
	{
		return line1;
	}

	public void setLine1(String line1)
	{
		this.line1 = line1;
	}

	public String getLine2()
	{
		return line2;
	}

	public void setLine2(String line2)
	{
		this.line2 = line2;
	}

	public String getArea()
	{
		return area;
	}

	public void setArea(String area)
	{
		this.area = area;
	}

	public String getCity()
	{
		return city;
	}

	public void setCity(String city)
	{
		this.city = city;
	}

	public int getPinCode()
	{
		return pinCode;
	}

	public void setPinCode(int pinCode)
	{
		this.pinCode = pinCode;
	}
}
