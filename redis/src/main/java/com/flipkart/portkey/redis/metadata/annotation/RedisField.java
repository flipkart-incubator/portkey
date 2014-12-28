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
@Target (ElementType.FIELD)
public @interface RedisField
{
	public String attributeName();

	public boolean isShardKey() default false;

	public boolean isJson() default false;

	public boolean isJsonList() default false;
}
