package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

public class StringMapWritable implements Writable {
	
	public LinkedList<StringPairWritable> data;
	
	public StringMapWritable() {
		data = new LinkedList<StringPairWritable>();
	}

	@Override
	public void readFields(DataInput r) throws IOException {
		// TODO Auto-generated method stub
		int num = r.readInt();
		for(int i = 0; i < num; i++) {
			StringPairWritable spw = new StringPairWritable();
			spw.readFields(r);
			data.add(spw);
		}
	}

	@Override
	public void write(DataOutput w) throws IOException {
		// TODO Auto-generated method stub
		w.writeInt(data.size());
		for (StringPairWritable spw : data) {
			spw.write(w);
		}
	}

}
