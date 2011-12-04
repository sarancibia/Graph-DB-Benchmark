package data_generator.objects;
import java.net.URL;
import java.util.List;

import data_generator.database.MYSQL_DATABASE;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.Source;


public class Profile {
	public String url;
	public Source profile;
	
	public String nombre;
	public String edad;
	public String sexo;
	public String cumpleanos;
	public String ubicacion;
	public String ciudad_natal;
	public String estado_civil;
	public String buscando;
	
	public String idiomas;
	
	public String acerca_de_mi;
	public String intereses;
	public String musica_favorita;
	public String peliculas_favoritas;
	public String programas_de_tv_favoritos;
	public String libros_favoritos;
	public String cita_favorita;
	
	public Profile(){
		
	}
	public Profile(String url){
		this.url = url;
		
		this.edad = "";
		this.sexo = "";
		this.cumpleanos = "";
		this.ubicacion = "";
		this.ciudad_natal = "";
		this.estado_civil = "";
		this.buscando = "";
		this.acerca_de_mi = "";
		this.intereses = "";
		this.musica_favorita = "";
		this.peliculas_favoritas = "";
		this.programas_de_tv_favoritos = "";
		this.libros_favoritos = "";
		this.cita_favorita = "";
		this.idiomas = "";
		
		try {
			this.profile = new Source(new URL("http://hi5.com" + this.url));
			Element user = profile.getElementById("headerName");
	
			this.nombre = user.getTextExtractor().toString();
			
			
			try{
				List<Element> info_boxs = profile.getAllElementsByClass("info-box");
				for(Element info_box : info_boxs){
					try{
						this.setAtributo(info_box.getFirstElementByClass("box_profile_info_small_heading").getTextExtractor().toString(),
								info_box.getFirstElementByClass("box_profile_info_small_content").getTextExtractor().toString());
					}catch(Exception e){}
				}
			}catch(Exception e){}
			
			try{
				Element sections = profile.getElementById("lifestyle");

				this.setAtributo(sections.getFirstElementByClass("bg_title box_profile_info_large_heading").getTextExtractor().toString(),
						sections.getFirstElementByClass("box_profile_info_small_content").getTextExtractor().toString());
			}catch(Exception e){}
			
			try{
				Element interests = profile.getElementById("interests");
			
				try{
					this.setAtributo(interests.getFirstElementByClass("bg_title box_profile_info_large_heading").getTextExtractor().toString(),
							interests.getFirstElementByClass("box_profile_info_small_content").getTextExtractor().toString());
				}catch(Exception e){}
				
				List<Element> l_interest = interests.getAllElementsByClass("subsection");
				for(Element l_interest_item : l_interest){
					try{				
						this.setAtributo(l_interest_item.getFirstElementByClass("box_profile_info_small_heading").getTextExtractor().toString(),
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
	private void setAtributo(String atributo, String contenido) {
		if(atributo.equals("Age")){
			this.edad = contenido;			
		}
		else if(atributo.equals("Location")){
			this.ubicacion = contenido;
		}
		else if(atributo.equals("Birthday")){
			this.cumpleanos = contenido;
		}
		else if(atributo.equals("Looking To")){
			this.buscando = contenido;
		}
		else if(atributo.equals("About Me")){
			this.acerca_de_mi = contenido;
		}
		else if(atributo.equals("Interests")){
			this.intereses = contenido;
		}
		else if(atributo.equals("Favorite Music")){
			this.musica_favorita = contenido;
		}
		else if(atributo.equals("Favorite Movies")){
			this.peliculas_favoritas = contenido;
		}
		else if(atributo.equals("Favorite TV Shows")){
			this.programas_de_tv_favoritos = contenido;
		}
		else if(atributo.equals("Favorite Books")){
			this.libros_favoritos = contenido;
		}
		else if(atributo.equals("Favorite Quote")){
			this.cita_favorita = contenido;
		}
		else if(atributo.equals("Languages")){
			this.idiomas = contenido;
		}
		else if(atributo.equals("Sex /  Age") || atributo.equals("Sex")){
			this.sexo = contenido;
		}
		else{
			System.out.println(atributo + " - " + contenido);
		}
		
	}
}
