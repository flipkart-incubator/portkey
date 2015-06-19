package com.flipkart.portkey.rdbms.metadata.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author santosh.p
 */

@Retention (RetentionPolicy.RUNTIME)
@Target (ElementType.TYPE)
public @interface Join
{
	public String databaseName();

	public JoinTable[] tableList();

	public JoinCriteria[] joinCriteriaList();

	public String shardKeyField();
}
