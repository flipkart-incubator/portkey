/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class QueryNotSupportedException extends QueryExecutionException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2141092327367581030L;

	/**
	 * 
	 */
	public QueryNotSupportedException()
	{
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public QueryNotSupportedException(String message)
	{
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public QueryNotSupportedException(Throwable cause)
	{
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public QueryNotSupportedException(String message, Throwable cause)
	{
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public QueryNotSupportedException(String message, Throwable cause, boolean enableSuppression,
	        boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
