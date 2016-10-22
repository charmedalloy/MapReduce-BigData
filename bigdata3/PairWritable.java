package aval.bigdata3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class PairWritable implements WritableComparable<PairWritable> {
	public Long p1;
	public Long p2;

	public PairWritable(Long p1, Long p2) {
		this.p1 = p1;
		this.p2 = p2;
	}

	public PairWritable() {
		this(-1L, -1L);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(p1);
		out.writeLong(p2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		p1 = in.readLong();
		p2 = in.readLong();
	}

	@Override
	public String toString() {
		return Long.toString(p1) + " , " + Long.toString(p2);
	}

	@Override
	public int compareTo(PairWritable o) {

		int cmp = this.p1.compareTo(o.p1);

		if (cmp != 0) {
			return cmp;
		}
		return this.p2.compareTo(o.p2);
		// TODO Auto-generated method stub

	}

	@Override
	public int hashCode() {
		return p1.hashCode() + p2.hashCode();

	}
}