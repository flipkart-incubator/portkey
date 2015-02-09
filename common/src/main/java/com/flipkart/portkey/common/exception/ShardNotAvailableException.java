/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class ShardNotAvailableException extends QueryExecutionException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3231739591791619783L;

	/**
	 * 
	 */
	public ShardNotAvailableException()
	{
	}

	/**
	 * @param message
	 */
	public ShardNotAvailableException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public ShardNotAvailableException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ShardNotAvailableException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
