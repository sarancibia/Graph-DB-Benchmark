package data_generator.objects;

public class Relationship {
	public String profile_a;
	public String profile_b;

	public Relationship(String profile_a, String profile_b){
		this.profile_a = profile_a;
		this.profile_b = profile_b;
	}
	
	public boolean equals(Object o){
		if(o instanceof Relationship){
			Relationship a = (Relationship) o;
			if(a.profile_a.equals(this.profile_a) && a.profile_b.equals(this.profile_b) || a.profile_a.equals(this.profile_b) && a.profile_b.equals(this.profile_a)){
				return true;
			}
		}
		return false;
	}
	
	public String toString(){
		return this.profile_a + " " + profile_b;
	}
}
