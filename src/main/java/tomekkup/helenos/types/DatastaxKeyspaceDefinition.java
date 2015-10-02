package tomekkup.helenos.types;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.BasicKeyspaceDefinition;

public class DatastaxKeyspaceDefinition extends BasicKeyspaceDefinition {
	private List<DatastaxColumnFamilyDefinition> columnFamilyList = new ArrayList<DatastaxColumnFamilyDefinition>();

	public List<DatastaxColumnFamilyDefinition> getColumnFamilyList() {
		return columnFamilyList;
	}

	public void setColumnFamilyList(List<DatastaxColumnFamilyDefinition> columnFamilyList) {
		this.columnFamilyList = columnFamilyList;
	}

	public void addColumnFamily(DatastaxColumnFamilyDefinition columnFamilyDefinition) {
		this.columnFamilyList.add(columnFamilyDefinition);
	}

}
