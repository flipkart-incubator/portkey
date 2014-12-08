/**
 * 
 */
package com.flipkart.portkey.metadata.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author santosh.p
 */
@Retention (RetentionPolicy.RUNTIME)
@Target (ElementType.METHOD)
public @interface RdbmsField
{
	public String columnName();

	public boolean isShardKey() default false;

	public boolean isPrimaryKey() default false;

	public boolean isUnique() default false;

	public boolean isJson() default false;

	public boolean isJsonList() default false;
}
