


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pairkey implements WritableComparable<Pairkey>{

	private Text hook,targets;
	
	public Pairkey() {
		hook= new Text();
		targets= new Text();
	}
	
    protected void init() {
    	hook = new Text();
    	targets = new Text();
    }
	
	public Pairkey(String realkey,String gram2or3) {
		this(new Text(realkey), new Text(gram2or3));
	}
	public Pairkey(Text realkey, Text gram2or3) {
		this.hook=realkey;
		this.targets=gram2or3;
	}

	public Text getrealkey() {
		return hook;
	}
	public void setrealkey(Text lname) {
		this.hook = lname;
	}
	public Text getFname() {
		return targets;
	}
	public void setskey(Text fname) {
		this.targets = fname;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		hook.readFields(in);
		targets.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		hook.write(out);
		targets.write(out);
	}

	@Override
	public int compareTo(Pairkey o) {
		// TODO Auto-generated method stub
		int cmp = hook.compareTo(o.hook);
		if (cmp==0){
			return targets.compareTo(o.targets);
		}
		return cmp;
	}




}
