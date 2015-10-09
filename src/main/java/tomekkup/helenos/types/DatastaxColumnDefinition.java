package tomekkup.helenos.types;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import tomekkup.helenos.types.Column.ColumnKeyType;

public class DatastaxColumnDefinition extends BasicColumnDefinition implements Comparable<DatastaxColumnDefinition>{

	private ColumnKeyType keyType;
	private int componentIndex;

	public ColumnKeyType getKeyType() {
		return keyType;
	}

	public void setKeyType(ColumnKeyType keyType) {
		this.keyType = keyType;
	}

	public int getComponentIndex() {
		return componentIndex;
	}

	public void setComponentIndex(int componentIndex) {
		this.componentIndex = componentIndex;
	}

	@Override
	public int compareTo(DatastaxColumnDefinition o) {
		if (this.getKeyType() == o.getKeyType()) {
			return Integer.compare(this.getComponentIndex(),o.getComponentIndex());
		}
		if (this.getKeyType() == ColumnKeyType.PARTITION_KEY) {
			return -1;
		}
		if (this.getKeyType() == ColumnKeyType.CLUSTERING_KEY && o.getKeyType() == ColumnKeyType.REGULAR) {
			return -1;
		}
		return 1;
	}


}
