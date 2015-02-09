/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class QueryExecutionException extends PortKeyException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1585073982189201178L;

	/**
	 * 
	 */
	public QueryExecutionException()
	{
	}

	/**
	 * @param message
	 */
	public QueryExecutionException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public QueryExecutionException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public QueryExecutionException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
