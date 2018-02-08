
public class Pattern{
	
 private String pattern;
 private boolean confirmed;
 private boolean core;
 
 
public Pattern(String pattern, boolean confirmed, boolean core) {
	super();
	this.pattern = pattern;
	this.confirmed = confirmed;
	this.core = core;
}


public String getPattern() {
	return pattern;
}


public void setPattern(String pattern) {
	this.pattern = pattern;
}


public boolean isConfirmed() {
	return confirmed;
}


public void setConfirmed(boolean confirmed) {
	this.confirmed = confirmed;
}


public boolean isCore() {
	return core;
}


public void setCore(boolean core) {
	this.core = core;
}



}