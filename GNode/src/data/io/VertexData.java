package data.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import node.GServer;

import org.apache.hadoop.io.Writable;

import utilities.HDFS_Utilities;
import data.io.DataPointers_Entity._DSInfo;
import data.io.VertexInfo._EdgeInfo;

public class VertexData implements Writable {

	private String id;
	private String graph_id;
	private LinkedList<_EdgeInfo> edge_List;

	private DataPointers_Entity pointer;

	private Graph_Schema schema;
	private HashMap<String, String> data;

	public VertexData() {
		this.edge_List = new LinkedList<_EdgeInfo>();
		this.schema = new Graph_Schema();
		this.data = new HashMap<String, String>();
	}

	public void initWithInfo(VertexInfo info) {
		this.id = info.getId();
		this.graph_id = info.getGraph_id();
		this.edge_List = (LinkedList<_EdgeInfo>) info.getEdge_List().clone();
		this.pointer = info.getPointer_List();
	}

	public void readData(GServer node) {
		// TODO ReadData from Data Layer
		HashMap<String, HashMap<Long, List<String>>> queries = new HashMap<>();
		for (data.io.Graph_Schema.Attribute attri : schema.getAttributes()) {
			_DSInfo dsi = this.pointer.data.get(attri.name);
			System.out.println("DSI GET " + dsi.attriName);
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
			System.out.println("target "+ target);
			String[] targets = target.split("@");
			String fsPath = node.getDataSetPath(targets[0]);
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
								System.out.println("INSERT TO DATA " + attri.name + "@" + result.get(key));
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
		this.graph_id = r.readUTF();

		int num = r.readInt();
		for (int i = 0; i < num; i++) {
			_EdgeInfo info = new _EdgeInfo();
			info.readFields(r);
			this.edge_List.add(info);
		}

		this.schema.readFields(r);

		num = r.readInt();
		for (int i = 0; i < num; i++) {
			String key = r.readUTF();
			String val = r.readUTF();
			this.data.put(key, val);

		}
	}

	@Override
	public void write(DataOutput w) throws IOException {
		// TODO Auto-generated method stub
		w.writeUTF(this.id);
		w.writeUTF(this.graph_id);

		w.writeInt(this.edge_List.size());

		for (_EdgeInfo info : this.edge_List) {
			info.write(w);
		}

		this.schema.write(w);

		w.writeInt(this.data.size());
		Set<String> keysSet = this.data.keySet();

		for (String key : keysSet) {
			w.writeUTF(key);
			w.writeUTF(this.data.get(key));
		}
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public Graph_Schema getSchema() {
		return schema;
	}

	public void setSchema(Graph_Schema schema) {
		this.schema = schema;
	}

	public HashMap<String, String> getData() {
		return data;
	}

	public void setData(HashMap<String, String> data) {
		this.data = data;
	}

}
