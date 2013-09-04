package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import ds.bplusTree.BPlusTree;

public class BPlusTreeStrStrWritable implements Writable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3289530422612438733L;

	private final int bPlusTreeDefault_Order = 16;
	
	private BPlusTree<String, String> data;
	
	public BPlusTreeStrStrWritable() {
		data = new BPlusTree<String, String>(bPlusTreeDefault_Order);
	}
	
	public BPlusTreeStrStrWritable(int factor) {
		data = new BPlusTree<String, String>(factor);
	}
	
	public BPlusTreeStrStrWritable(BPlusTree<String, String> parm) {
		data = parm;
	}

	@Override
	public void readFields(DataInput read) throws IOException {
		int num = read.readInt();
		for (int i = 0; i < num; i++) {
			data.insertOrUpdate(read.readUTF(), read.readUTF());
		}
	}

	@Override
	public void write(DataOutput write) throws IOException {
		Set<String> kset = data.getKeySet();
		write.writeInt(kset.size());
		for (String key : kset) {
			write.writeUTF(key);
			write.writeUTF(data.get(key));
		}
	}

	public BPlusTree<String, String> getData() {
		return data;
	}

	public void setData(BPlusTree<String, String> data) {
		this.data = data;
	}

}
