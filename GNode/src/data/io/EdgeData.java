package data.io;

import hdfs.HDFS_Utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import node.GServer;

import org.apache.hadoop.io.Writable;

import data.io.DataPointers_Entity._DSInfo;

public class EdgeData implements Writable {
	
	private String id;
	private String source_vertex_id;
	private String target_vertex_id;
	private Graph_Schema schema;
	private HashMap<String, Object> data;
	
	private DataPointers_Entity pointer;
	
	public EdgeData() {
		this.schema = new Graph_Schema();
		this.data = new HashMap<String, Object>();
	}
	
	public void initWithInfo(EdgeInfo info) {
		this.id = info.getId();
		this.source_vertex_id = info.getSource_vertex_id();
		this.target_vertex_id = info.getTarget_vertex_id();
		this.pointer = info.getPointer_List();
	}
	
	public void readData(GServer node) {
		HashMap<String, HashMap<Long, List<String>>> queries = new HashMap<>();
		for (data.io.Graph_Schema.Attribute attri : schema.getAttributes()) {
			_DSInfo dsi = this.pointer.data.get(attri.name);
			if (queries.get(dsi.dsId + "@" + dsi.dsSchemaId) == null) {
				HashMap<Long, List<String>> dsQueries = new HashMap<>();
				ArrayList<String> attriQueries = new ArrayList<>();
				attriQueries.add(dsi.attriName);
				dsQueries.put(dsi.offset, attriQueries);
				queries.put(dsi.dsId + "@" + dsi.dsSchemaId, dsQueries);
			} else {
				HashMap<Long, List<String>> dsQueries = queries.get(dsi.dsId
						+ "@" + dsi.dsSchemaId);
				if (dsQueries.get(dsi.offset) == null) {
					ArrayList<String> attriQueries = new ArrayList<>();
					attriQueries.add(dsi.attriName);
					dsQueries.put(dsi.offset, attriQueries);
				} else {
					dsQueries.get(dsi.offset).add(dsi.attriName);
				}
			}
		}
		for (String target : queries.keySet()) {
			String[] targets = target.split("@");
			String fsPath = node.getDataSetPath_Remote(targets[0]);
			Data_Schema ds = node.getDataSchema(targets[1]);
			if (fsPath != null && ds != null) {
				try {
					HashMap<String, String> result = HDFS_Utilities
							.getInstance().readDataSetByOffset(fsPath, ds,
									queries.get(target));
					for (String key : result.keySet()) {
						for (data.io.Graph_Schema.Attribute attri : schema
								.getAttributes()) {
							if (pointer.data.get(attri.name).attriName.equals(key)) {
								this.data.put(attri.name, result.get(key));
							}
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void readFields(DataInput r) throws IOException {
		// TODO Auto-generated method stub
		this.id = r.readUTF();
		this.source_vertex_id = r.readUTF();
		this.target_vertex_id = r.readUTF();
		
		this.schema.readFields(r);
		
		int num = r.readInt();
		for (int i = 0; i < num; i++) {
			String key = r.readUTF();
			Object val = null;
			for (Graph_Schema.Attribute attr : this.schema.getAttributes()) {
				if (attr.name.equals(key)) {
					switch (attr.dataType) {
					case bool:
						val = new Boolean(r.readBoolean());
						break;
					case integer:
						val = new Integer(r.readInt());
						break;
					case string:
						val = r.readUTF();
						break;
					default:
						val = r.readUTF();
						break;
					}
					break;
				}
			}
			if (val != null) {
				this.data.put(key, val);
			}
		}
	}

	@Override
	public void write(DataOutput w) throws IOException {
		// TODO Auto-generated method stub
		w.writeUTF(this.id);
		w.writeUTF(this.source_vertex_id);
		w.writeUTF(this.target_vertex_id);
		
		this.schema.write(w);
		
		w.writeInt(this.data.size());
		Set<String> keysSet = this.data.keySet();
		
		for(String key : keysSet) {
			w.writeUTF(key);
			for (Graph_Schema.Attribute attr : this.schema.getAttributes()) {
				if (attr.name.equals(key)) {
					switch (attr.dataType) {
					case bool:
						w.writeBoolean((Boolean) this.data.get(key));
						break;
					case integer:
						w.writeInt((Integer) this.data.get(key));
						break;
					case string:
						w.writeUTF((String) this.data.get(key));
						break;
					default:
						w.writeUTF((String) this.data.get(key));
						break;
					}
					break;
				}
			}
		}
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

	public Graph_Schema getSchema() {
		return schema;
	}

	public void setSchema(Graph_Schema schema) {
		this.schema = schema;
	}

	public HashMap<String, Object> getData() {
		return data;
	}

	public void setData(HashMap<String, Object> data) {
		this.data = data;
	}
	
	

}
