package data.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

public class VertexInfo implements Writable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6339097867301037112L;

	public static class _EdgeInfo implements Writable, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5896540784078661336L;
		public String id;
		public String target_vertex_id;
		public _EdgeInfo(String id, String vid) {
			this.id = id;
			this.target_vertex_id = vid;
		}
		public _EdgeInfo() {
			
		}
		@Override
		protected Object clone() throws CloneNotSupportedException {
			// TODO Auto-generated method stub
			return new _EdgeInfo(this.id, this.target_vertex_id);
		}
		@Override
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
			if (obj instanceof _EdgeInfo) {
				if (((_EdgeInfo) obj).id.equals(this.id)) {
					return true;
				}
			}
			return false;
		}
		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			this.id = arg0.readUTF();
			this.target_vertex_id = arg0.readUTF();
		}
		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeUTF(this.id);
			arg0.writeUTF(this.target_vertex_id);
		}

	}
	
	private String id;
	
	private String schema_id;
	private String graph_id;
	
	private LinkedList<_EdgeInfo> edge_List;
	private DataPointers_Entity pointer_List;
	
	public VertexInfo() {
		this.edge_List = new LinkedList<_EdgeInfo>();
		this.pointer_List = new DataPointers_Entity();
	}
	
	@Override
	public void readFields(DataInput read) throws IOException {
		// TODO Auto-generated method stub
		this.id = read.readUTF();
		this.schema_id = read.readUTF();
		this.graph_id = read.readUTF();
		
		int num = read.readInt();
		for (int i = 0; i < num; i++) {
			_EdgeInfo info = new _EdgeInfo();
			info.readFields(read);
			this.edge_List.add(info);
		}
		if (pointer_List != null)
			pointer_List.readFields(read);
	}

	@Override
	public void write(DataOutput write) throws IOException {
		// TODO Auto-generated method stub
		write.writeUTF(this.id);
		write.writeUTF(this.schema_id);
		write.writeUTF(this.graph_id);
		
		write.writeInt(this.edge_List.size());
		for (_EdgeInfo info : this.edge_List) {
			info.write(write);
		}
		if (pointer_List != null)
			pointer_List.write(write);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSchema_id() {
		return schema_id;
	}

	public void setSchema_id(String schema_id) {
		this.schema_id = schema_id;
	}

	public String getGraph_id() {
		return graph_id;
	}

	public void setGraph_id(String graph_id) {
		this.graph_id = graph_id;
	}

	public LinkedList<_EdgeInfo> getEdge_List() {
		return edge_List;
	}

	public void setEdge_List(LinkedList<_EdgeInfo> edge_List) {
		this.edge_List = edge_List;
	}

	public DataPointers_Entity getPointer_List() {
		return pointer_List;
	}

	public void setPointer_List(DataPointers_Entity pointer_List) {
		this.pointer_List = pointer_List;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VertexInfo other = (VertexInfo) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
	
	
	
	
	
}
