package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StringPairWritable implements Writable {
	
	public String key;
	public String value;
	
	public StringPairWritable() {
	
	}
	
	public StringPairWritable(String k, String v) {
		this.key = k;
		this.value = v;
	}

	@Override
	public void readFields(DataInput r) throws IOException {
		// TODO Auto-generated method stub
		this.key = r.readUTF();
		this.value = r.readUTF();
	}

	@Override
	public void write(DataOutput w) throws IOException {
		// TODO Auto-generated method stub
		w.writeUTF(this.key);
		w.writeUTF(this.value);
	}

}
