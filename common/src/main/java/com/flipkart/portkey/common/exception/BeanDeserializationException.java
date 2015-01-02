/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class BeanDeserializationException extends PortKeyRuntimeException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7317399041859505681L;

	/**
	 * 
	 */
	public BeanDeserializationException()
	{
	}

	/**
	 * @param message
	 */
	public BeanDeserializationException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public BeanDeserializationException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public BeanDeserializationException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public BeanDeserializationException(String message, Throwable cause, boolean enableSuppression,
	        boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
