/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class InvalidAnnotationException extends PortKeyRuntimeException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2247289950953561432L;

	/**
	 * 
	 */
	public InvalidAnnotationException()
	{
	}

	/**
	 * @param message
	 */
	public InvalidAnnotationException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public InvalidAnnotationException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InvalidAnnotationException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public InvalidAnnotationException(String message, Throwable cause, boolean enableSuppression,
	        boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
