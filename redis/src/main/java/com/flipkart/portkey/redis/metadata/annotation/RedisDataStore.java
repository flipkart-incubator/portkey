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
@Target (ElementType.TYPE)
public @interface RedisDataStore
{
	public int database() default 0;

	public String primaryKeyPattern();

	public String[] secondaryKeyPatterns() default {};
	
	public String shardKeyField();

}
