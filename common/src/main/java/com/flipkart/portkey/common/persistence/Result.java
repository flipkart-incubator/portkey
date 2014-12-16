/**
 * 
 */
package com.flipkart.portkey.common.persistence;

import java.util.Map;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.enumeration.DataStore;

/**
 * @author santosh.p
 */
public class Result
{
	Entity entity;
	Map<DataStore, Integer> updatesForDataStore;
}
