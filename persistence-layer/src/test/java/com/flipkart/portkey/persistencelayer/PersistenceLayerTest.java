/**
 * 
 */
package com.flipkart.portkey.persistencelayer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.flipkart.portkey.common.datastore.DataStoreConfig;
import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;
import com.flipkart.portkey.common.entity.persistence.ReadConfig;
import com.flipkart.portkey.common.entity.persistence.WriteConfig;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.FailureAction;
import com.flipkart.portkey.common.sharding.ShardIdentifierInterface;
import com.flipkart.portkey.common.sharding.ShardLifeCycleManagerInterface;
import com.flipkart.portkey.persistence.PersistenceLayer;

/**
 * @author santosh.p
 */
@RunWith (PowerMockRunner.class)
@PrepareForTest (PersistenceLayer.class)
public class PersistenceLayerTest
{
	private static final Logger logger = Logger.getLogger(PersistenceLayer.class);
	private EntityPersistencePreference defaultPersistencePreference;
	private Map<Class<? extends Entity>, EntityPersistencePreference> entityPersistencePreferenceMap;
	private Map<DataStoreType, DataStoreConfig> dataStoresMap;
	private ShardIdentifierInterface shardIdentifier;
	private ShardLifeCycleManagerInterface shardLifeCycleManager;

	@Test
	public void testGetEntityPersistencePreference() throws Exception
	{
		EntityPersistencePreference defaultPreference = new EntityPersistencePreference();
		ReadConfig readConfig = new ReadConfig();
		List<DataStoreType> readOrder = new ArrayList<DataStoreType>();
		readOrder.add(DataStoreType.RDBMS);
		readOrder.add(DataStoreType.REDIS);
		readConfig.setReadOrder(readOrder);

		WriteConfig writeConfig = new WriteConfig();
		List<DataStoreType> writeOrder = new ArrayList<DataStoreType>();
		writeOrder.add(DataStoreType.RDBMS);
		writeOrder.add(DataStoreType.REDIS);
		writeConfig.setWriteOrder(readOrder);
		writeConfig.setFailureAction(FailureAction.CONTINUE);

		defaultPreference.setReadConfig(readConfig);
		defaultPreference.setWriteConfig(writeConfig);

		PersistenceLayer pl = PowerMockito.spy(new PersistenceLayer());
		PowerMockito.doReturn(writeConfig).when(pl, "getWriteConfigForEntity");

	}
}
