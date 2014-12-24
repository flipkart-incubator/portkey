/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class MethodNotSupportedForDataStoreException extends PortKeyException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1876375205599947292L;

	/**
	 * 
	 */
	public MethodNotSupportedForDataStoreException()
	{
	}

	/**
	 * @param message
	 */
	public MethodNotSupportedForDataStoreException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public MethodNotSupportedForDataStoreException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public MethodNotSupportedForDataStoreException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public MethodNotSupportedForDataStoreException(String message, Throwable cause, boolean enableSuppression,
	        boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
