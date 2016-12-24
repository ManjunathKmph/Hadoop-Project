package com.manju.hadoopproject.dnamirror;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DNAMirrorKey implements WritableComparable<DNAMirrorKey> {
	
	private Text key;
	
	public DNAMirrorKey() {
		super();
		key = new Text();
	}
	
	public DNAMirrorKey(Text key) {
		super();
		this.key = key;
	}

	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public void write(DataOutput out) throws IOException {
		this.key.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.key.readFields(in);
	}

	public int compareTo(DNAMirrorKey o) {
		
		Text other = o.getKey();
		
		int mainResult = this.key.compareTo(other);
		
		if(mainResult == 0) {
			return mainResult;
		}
		String keyFirst = new StringBuilder(this.key.toString()).reverse().toString();
		
		int result = keyFirst.compareTo(other.toString());		
		if(result == 0) {
			return result;
		}
		
		String keySecond = new StringBuilder(o.getKey().toString()).reverse().toString();
		
		result =  this.key.toString().compareTo(keySecond);		
		if(result == 0) {
			return result;
		}
		return mainResult;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DNAMirrorKey other = (DNAMirrorKey) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
	
	

}
