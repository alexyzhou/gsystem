package rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

import data.io.EdgeInfo;
import data.io.VertexInfo;

public interface GMasterProtocol extends VersionedProtocol {
	
	//For Graph
	
		//Graph Data
	
			//insert
			public String findTargetGServer_Store(VertexInfo infomation);
			public String findTargetGServer_StoreEdge(EdgeInfo information);
			//update
			//remove
			//query
	
		//Graph Index
			
			//insert
			public void insertVertexInfoToIndex(String vid, String ip);
			public void insertEdgeInfoToIndex(String eid, String ip);
			//update
			//remove
			public void removeVertexFromIndex(String vid);
			public void removeEdgeFromIndex(String eid);
			//query
	
		//Graph Schema
			
	//End of Graph
			
	//For DataSet
			
		//DataSet PathIndex
			
			//insertOrUpdate
			public void notifyDataSet_Insert(String source, String dsID, String hdfsPath);
			//remove
			public void notifyDataSet_Remove(String source, String dsID);
			
		//DataSet Index
			
			//remove
			public void notifyDataSet_Index_Remove(String source, String dsID, String dschemaID, String attriName);
			
	//End of DataSet
			
			
	//For Server Management
		
		//For Index Server
			
			//insert
			//update
			public void requestToChangeIndexServer(String source_ip);
			//remove
			//query
	
	//End of Server Management
	
	public void stopService();
	
}
