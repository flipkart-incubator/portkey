/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class DataStoreNotRegisteredException extends PortKeyException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8907993243595317934L;

	/**
	 * 
	 */
	public DataStoreNotRegisteredException()
	{
	}

	/**
	 * @param message
	 */
	public DataStoreNotRegisteredException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public DataStoreNotRegisteredException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public DataStoreNotRegisteredException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public DataStoreNotRegisteredException(String message, Throwable cause, boolean enableSuppression,
	        boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
