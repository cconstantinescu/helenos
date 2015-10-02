package tomekkup.helenos.types;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;

public class DatastaxColumnFamilyDefinition extends BasicColumnFamilyDefinition{

	private final List<DatastaxColumnDefinition> columnDefinitions = new ArrayList<DatastaxColumnDefinition>();
	
	public List<DatastaxColumnDefinition> getColumnDefinitions() {
		return columnDefinitions;
	}

	public void addColumnDefinition(DatastaxColumnDefinition columnDefinition) {
		this.columnDefinitions.add(columnDefinition);
	}
}
