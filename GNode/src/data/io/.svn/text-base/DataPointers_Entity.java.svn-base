package data.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class DataPointers_Entity implements Writable, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3363469105220881532L;
	public HashMap<String, _DSInfo> data;
	
	public DataPointers_Entity() {
		data = new HashMap<String, _DSInfo>();
	}

	public static class _DSInfo implements Writable, Serializable {

		private static final long serialVersionUID = 1837967026199737753L;
		public String dsId;
		public String dsSchemaId;
		public Long offset;
		public String attriName;

		public _DSInfo(String id, String schema, Long offset, String attriName) {
			this.dsId = id;
			this.dsSchemaId = schema;
			this.offset = offset;
			this.attriName = attriName;
		}
		
		public _DSInfo() {
		}

		@Override
		public void readFields(DataInput read) throws IOException {
			// TODO Auto-generated method stub
			this.dsId = read.readUTF();
			this.dsSchemaId = read.readUTF();
			this.offset = new Long(read.readLong());
			this.attriName = read.readUTF();
		}

		@Override
		public void write(DataOutput write) throws IOException {
			// TODO Auto-generated method stub
			write.writeUTF(this.dsId);
			write.writeUTF(this.dsSchemaId);
			write.writeLong(this.offset);
			write.writeUTF(this.attriName);
		}

	}

	@Override
	public void readFields(DataInput read) throws IOException {
		// TODO Auto-generated method stub
		int num = read.readInt();
		
		for (int i = 0; i < num; i++) {
			String attriName = read.readUTF();
			_DSInfo attri_DSInfos = new _DSInfo();
			attri_DSInfos.readFields(read);
			
			this.data.put(attriName, attri_DSInfos);
		}
	}



	@Override
	public void write(DataOutput write) throws IOException {
		// TODO Auto-generated method stub
		write.writeInt(this.data.size());
		
		Set<String> keys = this.data.keySet();
		
		for (String key : keys) {
			write.writeUTF(key);
			_DSInfo attri_DSInfos = this.data.get(key);
			attri_DSInfos.write(write);
		}
	}
}
