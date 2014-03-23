package data.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Graph_Schema implements Serializable, Writable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4478155098115621578L;
	
	public class Attribute implements Serializable, Writable{
		/**
		 * 
		 */
		private static final long serialVersionUID = -6316384602468026069L;
		public String name;
		public DS_DataType dataType;
		//public String related_dsID;
		
		public Attribute() {
			
		}
		public Attribute(String name, DS_DataType type) {
			this.name = name;
			this.dataType = type;
		}
		@Override
		public void readFields(DataInput r) throws IOException {
			// TODO Auto-generated method stub
			this.name = r.readUTF();
			this.dataType = DS_DataType.valueOf(r.readUTF());
		}
		@Override
		public void write(DataOutput w) throws IOException {
			// TODO Auto-generated method stub
			w.writeUTF(this.name);
			w.writeUTF(this.dataType.toString());
		}
	}
	
	private String sId;
	private ArrayList<Attribute> attributes;
	
	@Override
	public void readFields(DataInput r) throws IOException {
		// TODO Auto-generated method stub
		this.sId = r.readUTF();
		int num = r.readInt();
		for (int i = 0; i < num; i++) {
			Attribute attribute = new Attribute();
			attribute.readFields(r);
			this.attributes.add(attribute);
		}
	}
	@Override
	public void write(DataOutput w) throws IOException {
		// TODO Auto-generated method stub
		w.writeUTF(this.sId);
		w.writeInt(this.attributes.size());
		for (Attribute attr : this.attributes) {
			attr.write(w);
		}
	}
	public Graph_Schema() {
		this.attributes = new ArrayList<Attribute>();
	}
	public Graph_Schema(String id, ArrayList<Attribute> ats) {
		this.sId = id;
		this.attributes = ats;
	}
	public String getsId() {
		return sId;
	}
	public void setsId(String sId) {
		this.sId = sId;
	}
	public ArrayList<Attribute> getAttributes() {
		return attributes;
	}
	public void setAttributes(ArrayList<Attribute> attributes) {
		this.attributes = attributes;
	}
	
}
