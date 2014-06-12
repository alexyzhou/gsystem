package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.Writable;

public class TraverseJobParameters implements Writable {
	
	public enum TraversalMethod {
		BFS,
		DFS
	}
	public UUIDWritable jobID;
	public TraversalMethod method;
	public int maxdepth;
	public long beginTime;
	public String client_ip;
	
	public TraverseJobParameters() {
		jobID = new UUIDWritable();
	}

	public TraverseJobParameters(UUID jobID, TraversalMethod method,
			int maxdepth, long beginTime, String client_IP) {
		this.jobID = new UUIDWritable(jobID);
		this.method = method;
		this.maxdepth = maxdepth;
		this.beginTime = beginTime;
		this.client_ip = client_IP;
	}


	@Override
	public void readFields(DataInput input) throws IOException {
		jobID.readFields(input);
		switch (input.readInt()) {
		case 0:
			//BFS
			method = TraversalMethod.BFS;
			break;
		case 1:
			//DFS
			method = TraversalMethod.DFS;
			break;
		default:
			method = TraversalMethod.BFS;
			break;
		}
		maxdepth = input.readInt();
		beginTime = input.readLong();
		client_ip = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		jobID.write(output);
		switch (method) {
		case BFS:
			output.writeInt(0);
			break;
		case DFS:
			output.writeInt(1);
			break;
		default:
			output.writeInt(0);
			break;
		}
		output.writeInt(maxdepth);
		output.writeLong(beginTime);
		output.writeUTF(client_ip);
	}

}
