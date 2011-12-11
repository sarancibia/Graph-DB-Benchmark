package data_generator.objects;
import java.net.URL;
import java.util.List;

import data_generator.database.MYSQL_DATABASE;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.Source;


public class Profile {
	public String url;
	public Source profile;
	
	public String name;
	public String age;
	public String sex;
	public String birthday;
	public String location;
	public String looking_to;
	public String languages;
	public String about_me;
	public String interests;
	public String favorite_music;
	public String favorite_movies;
	public String favorite_tv_shows;
	public String favorite_books;
	public String favorite_quote;
	
	public Profile(){	
	}
	public Profile(String url){
		this.url = url;
		
		this.age = "";
		this.sex = "";
		this.birthday = "";
		this.location = "";
		this.looking_to = "";
		this.about_me = "";
		this.interests = "";
		this.favorite_music = "";
		this.favorite_movies = "";
		this.favorite_tv_shows = "";
		this.favorite_books = "";
		this.favorite_quote = "";
		this.languages = "";
		
		try {
			this.profile = new Source(new URL("http://hi5.com" + this.url));
			Element user = profile.getElementById("headerName");
	
			this.name = user.getTextExtractor().toString();
			
			
			try{
				List<Element> info_boxs = profile.getAllElementsByClass("info-box");
				for(Element info_box : info_boxs){
					try{
						this.setAttribute(info_box.getFirstElementByClass("box_profile_info_small_heading").getTextExtractor().toString(),
								info_box.getFirstElementByClass("box_profile_info_small_content").getTextExtractor().toString());
					}catch(Exception e){}
				}
			}catch(Exception e){}
			
			try{
				Element sections = profile.getElementById("lifestyle");

				this.setAttribute(sections.getFirstElementByClass("bg_title box_profile_info_large_heading").getTextExtractor().toString(),
						sections.getFirstElementByClass("box_profile_info_small_content").getTextExtractor().toString());
			}catch(Exception e){}
			
			try{
				Element interests = profile.getElementById("interests");
			
				try{
					this.setAttribute(interests.getFirstElementByClass("bg_title box_profile_info_large_heading").getTextExtractor().toString(),
							interests.getFirstElementByClass("box_profile_info_small_content").getTextExtractor().toString());
				}catch(Exception e){}
				
				List<Element> l_interest = interests.getAllElementsByClass("subsection");
				for(Element l_interest_item : l_interest){
					try{				
						this.setAttribute(l_interest_item.getFirstElementByClass("box_profile_info_small_heading").getTextExtractor().toString(),
								l_interest_item.getFirstElementByClass("box_profile_info_small_content").getTextExtractor().toString());
					}catch(Exception e){}
				}
			}catch(Exception e){}

			
			MYSQL_DATABASE.save_profile(this);
			
		} catch (Exception e) {
			if(this.url != null){
				MYSQL_DATABASE.save_private_profile(url);
			}
		}
	}
	private void setAttribute(String attribute, String content) {
		if(attribute.equals("Age")){
			this.age = content;			
		}
		else if(attribute.equals("Location")){
			this.location = content;
		}
		else if(attribute.equals("Birthday")){
			this.birthday = content;
		}
		else if(attribute.equals("Looking To")){
			this.looking_to = content;
		}
		else if(attribute.equals("About Me")){
			this.about_me = content;
		}
		else if(attribute.equals("Interests")){
			this.interests = content;
		}
		else if(attribute.equals("Favorite Music")){
			this.favorite_music = content;
		}
		else if(attribute.equals("Favorite Movies")){
			this.favorite_movies = content;
		}
		else if(attribute.equals("Favorite TV Shows")){
			this.favorite_tv_shows = content;
		}
		else if(attribute.equals("Favorite Books")){
			this.favorite_books = content;
		}
		else if(attribute.equals("Favorite Quote")){
			this.favorite_quote = content;
		}
		else if(attribute.equals("Languages")){
			this.languages = content;
		}
		else if(attribute.equals("Sex /  Age") || attribute.equals("Sex")){
			this.sex = content;
		}
		else{
			System.out.println(attribute + " - " + content);
		}
		
	}
	public String toString(){
		return name;
	}
	public void fix() {
		try{
			if(name != null){
				name = name.substring(0, name.indexOf("("));
				name = name.trim();
			}
		}catch(Exception e){}
		
		try{
			if(sex != null){
				String age = sex.substring(sex.indexOf("/"));
				age = age.replace("/", "");
				
				sex = sex.substring(0, sex.indexOf("/"));
				
				sex = sex.trim();
				age = age.trim();
			}
		}catch(Exception e){}
		
	}
}

