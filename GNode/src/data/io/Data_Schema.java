package data.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Data_Schema implements Serializable, Writable {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = -3784386962117799235L;
	
	private String id;
	private char seperator;
	private ArrayList<ColumnDescription> columns;
	
	public Data_Schema() {
		columns = new ArrayList<ColumnDescription>();
	}
	
	@Override
	public void readFields(DataInput read) throws IOException {
		this.id = read.readUTF();
		this.seperator = read.readChar();
		int num = read.readInt();
		columns.clear();
		for (int i = 0; i < num; i++) {
			ColumnDescription cd = new ColumnDescription();
			cd.readFields(read);
			columns.add(cd);
		}
	}

	@Override
	public void write(DataOutput write) throws IOException {
		write.writeUTF(this.id);
		write.writeChar(this.seperator);
		write.writeInt(columns.size());
		for (ColumnDescription cd : columns) {
			cd.write(write);
		}
	}
	
	public class ColumnDescription implements Serializable, Writable{
		/**
		 * 
		 */
		private static final long serialVersionUID = -7314564490453201902L;
		public String name;
		public DS_DataType type;
		public String range;
		public boolean index_flag;
		
		public ColumnDescription() {
			
		}
		
		public ColumnDescription(String name, DS_DataType type, String range, boolean flag) {
			this.name = name;
			this.type = type;
			this.range = range;
			this.index_flag = flag;
		}

		@Override
		public void readFields(DataInput read) throws IOException {
			this.name = read.readUTF();
			this.type = DS_DataType.valueOf(read.readUTF());
			this.range = read.readUTF();
			this.index_flag = read.readBoolean();
		}

		@Override
		public void write(DataOutput write) throws IOException {
			write.writeUTF(this.name);
			write.writeUTF(this.type.toString());
			write.writeUTF(this.range);
			write.writeBoolean(this.index_flag);
		}
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public char getSeperator() {
		return seperator;
	}

	public void setSeperator(char seperator) {
		this.seperator = seperator;
	}

	public ArrayList<ColumnDescription> getColumns() {
		return columns;
	}

	public void setColumns(ArrayList<ColumnDescription> columns) {
		this.columns = columns;
	}
	
	

}
