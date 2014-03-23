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
	public UUID jobID;
	public TraversalMethod method;
	public int maxdepth;
	
	public TraverseJobParameters() {
		jobID = null;
	}

	public TraverseJobParameters(UUID jobID, TraversalMethod method,
			int maxdepth) {
		this.jobID = jobID;
		this.method = method;
		this.maxdepth = maxdepth;
	}


	@Override
	public void readFields(DataInput input) throws IOException {
		
		jobID = UUID.fromString(input.readUTF());
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
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeUTF(jobID.toString());
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
	}

}
