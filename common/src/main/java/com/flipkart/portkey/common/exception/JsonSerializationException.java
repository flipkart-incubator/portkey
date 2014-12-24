/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class JsonSerializationException extends PortKeyException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4261198445963044487L;

	/**
	 * 
	 */
	public JsonSerializationException()
	{
	}

	/**
	 * @param message
	 */
	public JsonSerializationException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public JsonSerializationException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public JsonSerializationException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public JsonSerializationException(String message, Throwable cause, boolean enableSuppression,
	        boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
