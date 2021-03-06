package rpc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import data.io.Data_Schema;
import data.io.EdgeData;
import data.io.EdgeInfo;
import data.io.Graph_Schema;
import data.io.VertexData;
import data.io.VertexInfo;
import data.writable.BPlusTreeStrStrWritable;
import data.writable.EdgeCollectionWritable;
import data.writable.StringMapWritable;
import data.writable.TraverseJobParameters;
import data.writable.TraverseJobValuePairWritable;
import data.writable.UUIDWritable;
import data.writable.VertexCollectionWritable;
import ds.index.BinarySearchStringIndex;

public interface GServerProtocol extends VersionedProtocol {
		
//For Graph
	
	//Graph Data

		//insert
		public String storeVertexAndUpdateIndex(VertexInfo vdata);
		public String storeEdgeAndUpdateVertex(EdgeInfo edata);
		public String storeVertexList(VertexCollectionWritable vdata);
		public String storeEdgeList(EdgeCollectionWritable edata);
		public float getMarkForTargetVertex(VertexInfo info);
		//update
		public boolean updateVertexInfo(VertexInfo info);
		//remove
		public boolean removeVertex(String id);
		public boolean removeEdge(String id, String source_vertex_id);
		//query
		public VertexInfo getVertexInfo(String id);
		public VertexData getVertexData(String id);
		public EdgeInfo getEdgeInfo(String id);
		public EdgeData getEdgeData(String id);
		//query graph
		public void traverseGraph_Async(String starting_v_id, TraverseJobParameters param);
			//private methods
			public void traverseGraph_Remote_Async(TraverseJobValuePairWritable[] array, TraverseJobParameters param, IntWritable currentLevel, String parentIP, UUIDWritable parentJobID);
			public void traverseGraph_NotifyFinish(TraverseJobValuePairWritable[] result, UUIDWritable jobID);

	//Graph Index
		
		//insert | update
		public void putVertexInfoToIndex(String vid, String targetIP);
		//public void putEdgeInfoToIndex(String eid, String targetIP);
		public void putVListToIndex(StringMapWritable map);
		//public void putEListToIndex(StringMapWritable map);
		//remove
		public void deleteVertexFromIndex(String vid);
		//public void deleteEdgeFromIndex(String eid);
		//query
		public String queryVertexToServer(String vid);
		public String queryEdgeToServer(String eid, String sourceID);
		public boolean EdgeExist(String id);
		//manage
		public double reportUsageMark();
		public void assignIndexServer(BPlusTreeStrStrWritable vertexIndex, BPlusTreeStrStrWritable dsPathIndex);
		public void announceIndexServer(String ip);

	//Graph Schema
		
		//insert | update
		public boolean insertOrUpdateSchema(String graphid, Graph_Schema gs);
		//remove
		public boolean removeSchema(String graph_id, String schema_id);
		//query
		
//End of Graph
		
//For DataSet Management
		
	// DataSet (Management by Zookeeper)
		
		//insert | NOUpdate
		public boolean insertDataSet(String dsID, String hdfsPath);
		public boolean insertDataSet_Sync(String dsID, String hdfsPath);
		//remove
		public boolean removeDataSet(String dsID);
		public boolean removeDataSet_Sync(String dsID);
		//query
		public String getDataSetPath(String dsID);
		
	// DataSet Index
		
		//create (Commit a MapReduce Job to analysis the dataset and create a bplusTree File)
		public String createDSIndex(String dsID, String dschemaID, String attriName);
		//update (remove and re-create)
		public String updateDSIndex(String dsID, String dschemaID, String attriName);
		//remove (remove HDFS file, update the cache)
		public String removeDSIndex(String dsID, String dschemaID, String attriName);
		public void removeDSIndex_Sync(String dsID, String dschemaID, String attriName);
		//manage
		public BinarySearchStringIndex getDSIndex(String dsID, String dschemaID, String attriName);
		
	// DataSet Schema
		
		//insert | update
		public boolean insertOrUpdateDataSchema(String dschemaID, Data_Schema ds);
		//remove
		public boolean removeDataSchema(String dschemaID);
		//query
		public Data_Schema getDataSchema(String dschemaID);
		
//End of DataSet
	
	
//For Server Management
public void stopService();
//End of Server Management
	
}
