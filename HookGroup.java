import java.util.ArrayList;
import java.util.HashMap;

public class HookGroup { // THIS IS CORPUS. THIS IS CREATED PER HOOKWORD.
 
 private String hookWord; // private Text hookWord
 private HashMap<String, ArrayList<Pattern>> group; // private MapWritable<Text, ArrayWritable(Text)>   AKA CLUSTER
  //   TARGET  PATTERN
 
public HookGroup(String hookWord) {
	super();
	this.hookWord = hookWord;
	this.group = new HashMap<String, ArrayList<Pattern>>();
}

public void addPatternToTarget(String target, Pattern pattern) {
	if (group.containsKey(target))
		group.get(target).add(pattern);
	else {
		group.put(target, new ArrayList<Pattern>());
		group.get(target).add(pattern);
	}
}

public String getHookWord() {
	return hookWord;
}

public void setHookWord(String hookWord) {
	this.hookWord = hookWord;
}

public HashMap<String, ArrayList<Pattern>> getGroup() {
	return group;
}

public void setGroup(HashMap<String, ArrayList<Pattern>> group) {
	this.group = group;
}



     
    // iterating the hashmap, choosing target (key) with the minimal number of patterns that is unconfirmed.
    //  Were the confirmed/unconfirmed field is set?
 
 
    // 1. FILE with HOOKWORD: TARGET~PATTERN | TARGET~PATTERN
    // 2. FILE WITH PATTERN: HOOKWORD | HOOKWORD | HOOKWORD
}