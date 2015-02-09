/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class PortKeyException extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3252837916636659705L;

	/**
	 * 
	 */
	public PortKeyException()
	{
	}

	/**
	 * @param message
	 */
	public PortKeyException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public PortKeyException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public PortKeyException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
