package tomekkup.helenos.service.impl;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.BasicKeyspaceDefinition;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import tomekkup.helenos.service.ClusterConfigAware;
import tomekkup.helenos.service.SchemaProvider;
import tomekkup.helenos.types.Column;
import tomekkup.helenos.types.Column.ColumnKeyType;
import tomekkup.helenos.types.DatastaxColumnDefinition;
import tomekkup.helenos.types.DatastaxColumnFamilyDefinition;
import tomekkup.helenos.types.DatastaxKeyspaceDefinition;
import tomekkup.helenos.types.JsonColumnFamilyDefinition;
import tomekkup.helenos.types.JsonKeyspaceDefinition;

@Component("datastaxSchemaProvider")
public class DatastaxSchemaProviderImpl extends AbstractProvider implements SchemaProvider, ClusterConfigAware {

	private static final Charset charset = Charset.forName("UTF-8");

	@Override
	public String describeClusterName() {
		return cluster.describeClusterName();
	}

	@Override
	public List<JsonKeyspaceDefinition> describeKeyspaces() {
		List<DatastaxKeyspaceDefinition> definitions = describeKeyspacesCQL();
		ArrayList<JsonKeyspaceDefinition> y = new ArrayList<JsonKeyspaceDefinition>();
		for (DatastaxKeyspaceDefinition kd : definitions) {
			y.add(mapper.map(kd, JsonKeyspaceDefinition.class));
		}
		return y;
	}

	@Override
	public JsonKeyspaceDefinition describeKeyspace(String keyspaceName) {
		List<DatastaxKeyspaceDefinition> definitions = describeKeyspaceCQL(keyspaceName);
		if (definitions.size() != 1) {
			return null;
		}
		return mapper.map(definitions.get(0), JsonKeyspaceDefinition.class);
	}

	@Override
	public JsonColumnFamilyDefinition describeColumnFamily(String keyspaceName, String columnFamilyName) {
		Session session = getNewCQLSession();
		try {
			List<DatastaxColumnFamilyDefinition> columns = describeColumnFamilyCQL(keyspaceName, columnFamilyName,
					session);
			if (columns.size() != 1) {
				return null;
			}
			return mapper.map(columns.get(0), JsonColumnFamilyDefinition.class);
		} finally {
			session.close();
		}
	}

	private List<DatastaxKeyspaceDefinition> describeKeyspacesCQL() {
		return describeKeyspaceCQL(null);
	}

	private List<DatastaxKeyspaceDefinition> describeKeyspaceCQL(String keyspaceName) {

		Session session = getNewCQLSession();
		try {
			String cql = "SELECT * FROM system.schema_keyspaces ";
			if (StringUtils.isNotBlank(keyspaceName)) {
				cql += " where keyspace_name = '" + keyspaceName + "'";
			}
			ResultSet result = session.execute(cql);
			List<DatastaxKeyspaceDefinition> definitions = new ArrayList<DatastaxKeyspaceDefinition>();
			for (Row keyspaceRow : result) {
				DatastaxKeyspaceDefinition def = new DatastaxKeyspaceDefinition();
				def.setDurableWrites(keyspaceRow.getBool("durable_writes"));
				def.setName(keyspaceRow.getString("keyspace_name"));
				def.setStrategyClass(keyspaceRow.getString("strategy_class"));
				Map<String, String> map = readMap(keyspaceRow.getString("strategy_options"));
				if (!map.isEmpty()) {
					String key = map.keySet().iterator().next();
					def.setStrategyOption(key, map.get(key));
				}
				if (map.containsKey("replication_factor")) {
					def.setReplicationFactor(Integer.parseInt(map.get("replication_factor").toString()));
				}
				List<DatastaxColumnFamilyDefinition> columnFamilyDefinitions = describeColumnFamiliesCQL(def.getName(),
						session);
				for (DatastaxColumnFamilyDefinition column : columnFamilyDefinitions) {
					def.addColumnFamily(column);
				}
				definitions.add(def);
			}
			return definitions;
		} finally {
			session.close();
		}
	}

	private List<DatastaxColumnFamilyDefinition> describeColumnFamiliesCQL(String keyspaceName, Session session) {
		return describeColumnFamilyCQL(keyspaceName, null, session);
	}

	private List<DatastaxColumnFamilyDefinition> describeColumnFamilyCQL(String keyspaceName, String columnFamily,
			Session session) {
		List<DatastaxColumnFamilyDefinition> columnFamilyDefinitions = new ArrayList<DatastaxColumnFamilyDefinition>();
		String cql = "SELECT * from system.schema_columnfamilies  where keyspace_name = '" + keyspaceName + "'";
		if (StringUtils.isNotBlank(columnFamily)) {
			cql += " and columnfamily_name = '" + columnFamily + "'";
		}
		ResultSet result = session.execute(cql);
		for (Row columnFamilityRow : result) {
			DatastaxColumnFamilyDefinition columnFamilyDefinition = new DatastaxColumnFamilyDefinition();
			columnFamilyDefinition.setName(columnFamilityRow.getString("columnfamily_name"));
			ResultSet resultColumnsDefinition = session
					.execute("SELECT * from system.schema_columns where keyspace_name = '" + keyspaceName
							+ "' and columnfamily_name='" + columnFamilyDefinition.getName() + "'");
			for (Row columnDefinitionRow : resultColumnsDefinition) {
				columnFamilyDefinition.addColumnDefinition(convertColumnDefinition(columnDefinitionRow));
			}
			Collections.sort(columnFamilyDefinition.getColumnDefinitions());
			columnFamilyDefinition.setColumnType(ColumnType.getFromValue(columnFamilityRow.getString("type")));
			columnFamilyDefinition.setComment(columnFamilityRow.getString("comment"));
			columnFamilyDefinition.setCompactionStrategy(columnFamilityRow.getString("compaction_strategy_class"));
			columnFamilyDefinition
					.setCompactionStrategyOptions(readMap(columnFamilityRow.getString("compaction_strategy_options")));
			String comparator = columnFamilityRow.getString("comparator");
			if (StringUtils.isNotBlank(comparator)) {
				columnFamilyDefinition.setComparatorType(ComparatorType.getByClassName(comparator));
			}
			// columnFamilyDefinition.setComparatorTypeAlias(row.getString("comparator"));
			columnFamilyDefinition
					.setCompressionOptions(readMap(columnFamilityRow.getString("compression_parameters")));
			columnFamilyDefinition.setDefaultValidationClass(columnFamilityRow.getString("default_validator"));
			columnFamilyDefinition.setGcGraceSeconds(columnFamilityRow.getInt("gc_grace_seconds"));
			// columnFamilyDefinition.setId(row.getInt("id"));
			columnFamilyDefinition.setKeyAlias(charset.encode(columnFamilityRow.getString("key_aliases")));
			// columnFamilyDefinition.setKeyCacheSavePeriodInSeconds(keyCacheSavePeriodInSeconds);
			// columnFamilyDefinition.setKeyCacheSize(keyCacheSize);
			columnFamilyDefinition.setKeyspaceName(keyspaceName);
			columnFamilyDefinition.setKeyValidationClass(columnFamilityRow.getString("key_validator"));
			columnFamilyDefinition.setMaxCompactionThreshold(columnFamilityRow.getInt("max_compaction_threshold"));
			columnFamilyDefinition.setMemtableFlushAfterMins(columnFamilityRow.getInt("memtable_flush_period_in_ms"));
			// columnFamilyDefinition.setMemtableOperationsInMillions(memtableOperationsInMillions);
			// columnFamilyDefinition.setMemtableThroughputInMb(memtableThroughputInMb);
			// columnFamilyDefinition.setMergeShardsChance(mergeShardsChance);
			columnFamilyDefinition.setMinCompactionThreshold(columnFamilityRow.getInt("min_compaction_threshold"));
			columnFamilyDefinition.setReadRepairChance(columnFamilityRow.getDouble("read_repair_chance"));
			// columnFamilyDefinition.setReplicateOnWrite(replicateOnWrite);
			// columnFamilyDefinition.setRowCacheKeysToSave(rowCacheKeysToSave);
			// columnFamilyDefinition.setRowCacheProvider(rowCacheProvider);
			// columnFamilyDefinition.setRowCacheSavePeriodInSeconds(rowCacheSavePeriodInSeconds);
			// columnFamilyDefinition.setRowCacheSize(rowCacheSize);
			String subcomparator = columnFamilityRow.getString("subcomparator");
			if (StringUtils.isNotBlank(subcomparator)) {
				columnFamilyDefinition.setSubComparatorType(ComparatorType.getByClassName(subcomparator));
			}
			// columnFamilyDefinition.setSubComparatorTypeAlias(alias);

			columnFamilyDefinitions.add(columnFamilyDefinition);
		}
		return columnFamilyDefinitions;
	}

	private DatastaxColumnDefinition convertColumnDefinition(Row columnDefinitionRow) {
		DatastaxColumnDefinition newColumnDefinition = new DatastaxColumnDefinition();
		newColumnDefinition.setName(charset.encode(columnDefinitionRow.getString("column_name")));
		newColumnDefinition.setIndexName(columnDefinitionRow.getString("index_name"));
		ColumnKeyType columnKeyType = ColumnKeyType.fromName(columnDefinitionRow.getString("type"));
		newColumnDefinition.setKeyType(columnKeyType);
		newColumnDefinition.setComponentIndex(columnDefinitionRow.getInt("component_index"));
		String validator = columnDefinitionRow.getString("validator");
		// handles the column types used in reversed indexes
		if (validator.startsWith(ReversedType.class.getName())) {
			validator = validator.substring(ReversedType.class.getName().length() + 1, validator.length() - 1);
		}
		newColumnDefinition.setValidationClass(validator);
		return newColumnDefinition;
	}

	private static Map<String, String> readMap(String value) {

		Map<String, String> result = new HashMap<String, String>();
		value = value.replace("}", "");
		value = value.replace("{", "");
		if (StringUtils.isNotBlank(value)) {
			String[] split = value.split(",");
			for (String s : split) {
				String[] split2 = s.split(":");
				result.put(split2[0].replace("\"", ""), split2[1].replace("\"", ""));
			}
		}
		return result;
	}

}
