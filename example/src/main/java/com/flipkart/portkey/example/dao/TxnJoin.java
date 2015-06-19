package com.flipkart.portkey.example.dao;

import lombok.Data;

import com.flipkart.portkey.common.entity.JoinEntity;
import com.flipkart.portkey.rdbms.metadata.annotation.Join;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinCriteria;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinField;
import com.flipkart.portkey.rdbms.metadata.annotation.JoinTable;

@Data
@Join (databaseName = "test_db", joinCriteriaList = {
        @JoinCriteria (srcTable = "Master", srcColumn = "id", destTable = "SaleDetail", destColumn = "id", criteriaValue = "=", joinType = "INNER JOIN"),
        @JoinCriteria (srcTable = "SaleDetail", srcColumn = "id", destTable = "UserDetail", destColumn = "id", criteriaValue = "=", joinType = "INNER JOIN")}, tableList = {
        @JoinTable (tableName = "Master", alias = "m"), @JoinTable (tableName = "SaleDetail", alias = "sd"),
        @JoinTable (tableName = "UserDetail", alias = "ud")}, shardKeyField = "id")
public class TxnJoin implements JoinEntity
{
	@JoinField (tableName = "Master", columnName = "id")
	private String id;
	@JoinField (tableName = "Master", columnName = "currency")
	private String currency;
	@JoinField (tableName = "Master", columnName = "amount")
	private long amount;
	@JoinField (tableName = "SaleDetail", columnName = "payment_method")
	private String paymentMethod;
	@JoinField (tableName = "SaleDetail", columnName = "bank")
	private String bank;
	@JoinField (tableName = "UserDetail", columnName = "name")
	private String name;
	@JoinField (tableName = "UserDetail", columnName = "address")
	private String address;

	@Override
	public String toString()
	{
		return "TxnJoin [id=" + id + ", currency=" + currency + ", amount=" + amount + ", paymentMethod="
		        + paymentMethod + ", bank=" + bank + ", name=" + name + ", address=" + address + "]";
	}
}
