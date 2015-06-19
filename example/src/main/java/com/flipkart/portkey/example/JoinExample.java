package com.flipkart.portkey.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.flipkart.portkey.common.exception.PortKeyException;
import com.flipkart.portkey.example.dao.TxnJoin;
import com.flipkart.portkey.persistence.PersistenceLayer;

public class JoinExample
{
	private static Logger logger = LoggerFactory.getLogger(JoinExample.class);
	private static PersistenceLayer pl;

	public static void main(String[] args)
	{
		ApplicationContext context =
		        new FileSystemXmlApplicationContext("src/main/resources/external/portkey-application-context.xml");
		pl = context.getBean(PersistenceLayer.class, "persistenceLayer");
		testJoin();
	}

	private static void testJoin()
	{
		List<String> attributes = new ArrayList<String>();
		attributes.add("id");
		attributes.add("currency");
		attributes.add("amount");
		attributes.add("paymentMethod");
		attributes.add("bank");
		attributes.add("name");
		attributes.add("address");
		Map<String, Object> criteria = new HashMap<String, Object>();
		criteria.put("id", "2");
		List<TxnJoin> l = null;
		try
		{
			l = pl.getByJoinCriteria(TxnJoin.class, attributes, criteria);
		}
		catch (PortKeyException e)
		{
			e.printStackTrace();
		}
		for (TxnJoin e : l)
		{
			System.out.println(e);
		}
	}
}
