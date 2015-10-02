package tomekkup.helenos.types;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import tomekkup.helenos.types.Column.ColumnKeyType;

public class DatastaxColumnDefinition extends BasicColumnDefinition{

	private ColumnKeyType keyType;

	public ColumnKeyType getKeyType() {
		return keyType;
	}

	public void setKeyType(ColumnKeyType keyType) {
		this.keyType = keyType;
	}


}
