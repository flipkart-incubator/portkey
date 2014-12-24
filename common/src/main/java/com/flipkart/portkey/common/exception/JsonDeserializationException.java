/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class JsonDeserializationException extends PortKeyException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7317399041859505681L;

	/**
	 * 
	 */
	public JsonDeserializationException()
	{
	}

	/**
	 * @param message
	 */
	public JsonDeserializationException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public JsonDeserializationException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public JsonDeserializationException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public JsonDeserializationException(String message, Throwable cause, boolean enableSuppression,
	        boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
