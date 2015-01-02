/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class BeanSerializationException extends PortKeyRuntimeException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4261198445963044487L;

	/**
	 * 
	 */
	public BeanSerializationException()
	{
	}

	/**
	 * @param message
	 */
	public BeanSerializationException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public BeanSerializationException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public BeanSerializationException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public BeanSerializationException(String message, Throwable cause, boolean enableSuppression,
	        boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
