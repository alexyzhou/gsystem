package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

import data.io.EdgeInfo;

public class EdgeCollectionWritable implements Writable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -400110141913708552L;
	public Collection<EdgeInfo> coll;
	
	public EdgeCollectionWritable(Collection<EdgeInfo> e) {
		coll = e;
	}
	
	public EdgeCollectionWritable() {
		coll = new LinkedList<EdgeInfo>();
	}

	@Override
	public void readFields(DataInput read) throws IOException {
		// TODO Auto-generated method stub
		int num = read.readInt();
		for (int i = 0; i < num; i++) {
			EdgeInfo e = new EdgeInfo();
			e.readFields(read);
			coll.add(e);
		}
	}

	@Override
	public void write(DataOutput write) throws IOException {
		// TODO Auto-generated method stub
		write.writeInt(coll.size());
		for(EdgeInfo e: coll) {
			e.write(write);
		}
	}

}
