package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TraverseJobTargetVertex implements Writable {
	
	public String prefix;
	public String id;
	
	public TraverseJobTargetVertex() {
		
	}

	public TraverseJobTargetVertex(String prefix, String id) {
		this.prefix = prefix;
		this.id = id;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		prefix = in.readUTF();
		id = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(prefix);
		out.writeUTF(id);
	}

}
