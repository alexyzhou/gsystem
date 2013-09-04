package data.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class EdgeInfo implements Writable, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2251734672630729662L;
	private String id;
	private String source_vertex_id;
	private String target_vertex_id;
	private String schema_id;
	
	private DataPointers_Entity pointer_List;

	public EdgeInfo() {
		this.pointer_List = new DataPointers_Entity();
	}
	
	@Override
	public void readFields(DataInput read) throws IOException {
		// TODO Auto-generated method stub
		this.id = read.readUTF();
		this.source_vertex_id = read.readUTF();
		this.target_vertex_id = read.readUTF();
		this.schema_id = read.readUTF();
		
		if (pointer_List != null)
			this.pointer_List.readFields(read);
	}

	@Override
	public void write(DataOutput write) throws IOException {
		// TODO Auto-generated method stub
		write.writeUTF(this.id);
		write.writeUTF(this.source_vertex_id);
		write.writeUTF(this.target_vertex_id);
		write.writeUTF(this.schema_id);
		
		if (pointer_List != null)
			this.pointer_List.write(write);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSource_vertex_id() {
		return source_vertex_id;
	}

	public void setSource_vertex_id(String source_vertex_id) {
		this.source_vertex_id = source_vertex_id;
	}

	public String getTarget_vertex_id() {
		return target_vertex_id;
	}

	public void setTarget_vertex_id(String target_vertex_id) {
		this.target_vertex_id = target_vertex_id;
	}

	public String getSchema_id() {
		return schema_id;
	}

	public void setSchema_id(String schema_id) {
		this.schema_id = schema_id;
	}

	public DataPointers_Entity getPointer_List() {
		return pointer_List;
	}

	public void setPointer_List(DataPointers_Entity pointer_List) {
		this.pointer_List = pointer_List;
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof EdgeInfo) {
			if (((EdgeInfo) obj).getId().equals(this.id)) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

}
