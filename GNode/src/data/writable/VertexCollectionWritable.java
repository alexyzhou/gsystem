package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

import data.io.VertexInfo;

public class VertexCollectionWritable implements Writable, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9039442941954911130L;
	public Collection<VertexInfo> coll;
	
	public VertexCollectionWritable(Collection<VertexInfo> e) {
		coll = e;
	}
	
	public VertexCollectionWritable() {
		coll = new LinkedList<VertexInfo>();
	}

	@Override
	public void readFields(DataInput read) throws IOException {
		// TODO Auto-generated method stub
		int num = read.readInt();
		for (int i = 0; i < num; i++) {
			VertexInfo e = new VertexInfo();
			e.readFields(read);
			coll.add(e);
		}
	}

	@Override
	public void write(DataOutput write) throws IOException {
		// TODO Auto-generated method stub
		write.writeInt(coll.size());
		
		for(VertexInfo e: coll) {
			e.write(write);
		}
	}

}
