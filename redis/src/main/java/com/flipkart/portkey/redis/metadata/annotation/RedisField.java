/**
 * 
 */
package com.flipkart.portkey.redis.metadata.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author santosh.p
 */
@Retention (RetentionPolicy.RUNTIME)
@Target (ElementType.METHOD)
public @interface RedisField
{
	public String attributeName();

	public boolean isJson();

	public boolean isJsonList();
}
