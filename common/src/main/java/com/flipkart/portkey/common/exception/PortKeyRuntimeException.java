/**
 * 
 */
package com.flipkart.portkey.common.exception;

/**
 * @author santosh.p
 */
public class PortKeyRuntimeException extends RuntimeException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6407840193521312456L;

	/**
	 * 
	 */
	public PortKeyRuntimeException()
	{
		super();
	}

	/**
	 * @param message
	 */
	public PortKeyRuntimeException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public PortKeyRuntimeException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public PortKeyRuntimeException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
