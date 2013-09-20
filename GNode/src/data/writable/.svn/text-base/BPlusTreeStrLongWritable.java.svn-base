package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import ds.bplusTree.BPlusTree;

public class BPlusTreeStrLongWritable implements Writable, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8785367784068753746L;

	private final int bPlusTreeDefault_Order = 64;
	
	private BPlusTree<String, Long> data;
	
	public BPlusTreeStrLongWritable() {
		data = new BPlusTree<String, Long>(bPlusTreeDefault_Order);
	}
	
	public BPlusTreeStrLongWritable(int factor) {
		data = new BPlusTree<String, Long>(factor);
	}
	
	public BPlusTreeStrLongWritable(BPlusTree<String, Long> parm) {
		data = parm;
	}

	@Override
	public void readFields(DataInput read) throws IOException {
		int num = read.readInt();
		for (int i = 0; i < num; i++) {
			data.insertOrUpdate(read.readUTF(), new Long(read.readLong()));
		}
	}

	@Override
	public void write(DataOutput write) throws IOException {
		Set<String> kset = data.getKeySet();
		write.writeInt(kset.size());
		for (String key : kset) {
			write.writeUTF(key);
			write.writeLong(data.get(key).longValue());
		}
	}

	public BPlusTree<String, Long> getData() {
		return data;
	}

	public void setData(BPlusTree<String, Long> data) {
		this.data = data;
	}
	
	

}
