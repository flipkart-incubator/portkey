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
@Target (ElementType.TYPE)
public @interface RdbmsTable
{
	public String tableName();

	public String databaseName();
}
