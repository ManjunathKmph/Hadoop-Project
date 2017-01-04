package com.manju.hadoopproject.dnamirror;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DNAMirrorKey implements WritableComparable<DNAMirrorKey> {
	
	private Text dna;
	
	public DNAMirrorKey() {
		super();
		this.dna = new Text();
	}
	
	public DNAMirrorKey(Text dna) {
		super();
		this.dna = dna;
	}

	public Text getDna() {
		return dna;
	}

	public void setDna(Text dna) {
		this.dna = dna;
	}

	public void write(DataOutput out) throws IOException {
		dna.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.dna.readFields(in);
	}

	public int compareTo(DNAMirrorKey o) {
		
		String other = o.getDna().toString();
		
		int mainResult = this.dna.toString().compareTo(other);
		
		if(mainResult == 0) {
			return mainResult;
		}
		String keyFirst = new StringBuilder(this.dna.toString()).reverse().toString();
		
		int result = keyFirst.compareTo(other);		
		if(result == 0) {
			return result;
		}
		
		String keySecond = new StringBuilder(o.getDna().toString()).reverse().toString();
		
		result =  this.dna.toString().compareTo(keySecond);		
		if(result == 0) {
			return result;
		}
		return mainResult;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dna == null) ? 0 : dna.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DNAMirrorKey other = (DNAMirrorKey) obj;
		if (dna == null) {
			if (other.dna != null)
				return false;
		} else if (!dna.equals(other.dna))
			return false;
		return true;
	}

}
