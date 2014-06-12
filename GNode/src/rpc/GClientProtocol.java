package rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface GClientProtocol extends VersionedProtocol {
	
	public void traverseDidFinished(String result);
	
}
