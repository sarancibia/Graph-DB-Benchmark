package data_generator.crawler;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.Source;

import data_generator.database.MYSQL_DATABASE;
import data_generator.exporter.FILE_OUTPUT;
import data_generator.objects.Profile;

public class CRAWLER_HI5 {
	
	public static void main(String[] args) {
		while(true){
			if(MYSQL_DATABASE.queue_isEmpty()){
				getSemillas();
			}
			while(!MYSQL_DATABASE.queue_isEmpty()){
				ArrayList<String> nodos = MYSQL_DATABASE.dequeue();
				
				while(nodos.size() > 0){
					String nodo = nodos.get(0);
					nodos.remove(0);
					
					getFriends(nodo);
				}
			}
		}
	}
	
	private static void getFriends(String nodo) {	
		try {
			Source profile = new Source(new URL("http://hi5.com" + nodo));
			Element friends_block = profile.getElementById("friends");
			String friends_url = friends_block.getFirstElement("h2").getFirstElement("a").getAttributeValue("href");

			
			do{
				Source friends = new Source(new URL("http://hi5.com" + friends_url));
				List <Element> friends_list = friends.getAllElementsByClass("friend-name");
				
				for(Element friend : friends_list){
					try{
						if(!MYSQL_DATABASE.exist(friend.getFirstElement("a").getAttributeValue("href"))){
							Profile p = new Profile(friend.getFirstElement("a").getAttributeValue("href"));
							if(p.nombre != null)
								MYSQL_DATABASE.enqueue(friend.getFirstElement("a").getAttributeValue("href"));
						}
						MYSQL_DATABASE.save_friends(nodo, friend.getFirstElement("a").getAttributeValue("href"));
					}
					catch(Exception e){
					}
				}
				
				List <Element> links = friends.getAllElementsByClass("link_pagination_arrow");
				if(links.get(links.size()-1).getAttributeValue("href").contains("next")){
					friends_url = links.get(links.size()-1).getAttributeValue("href");
				}
				else{
					friends_url = null;
				}
			} while(friends_url != null);
		} catch (Exception e) {
			System.out.println("No se pueden extraer amigos de " + nodo);
			FILE_OUTPUT.escribir("\t No se pueden extraer amigos de " + nodo);
		}
	}
	public static void getSemillas(){		
		try {
			Source s = new Source(new URL("http://hi5.com"));
			List<Element> amigos = s.getAllElementsByClass("friend");
			
			for(Element amigo : amigos){
				String url = amigo.getFirstElement("a").getAttributeValue("href");
				if(!MYSQL_DATABASE.exist(url)){
					MYSQL_DATABASE.enqueue(url);
					new Profile(url);
				}
			}
		} catch (Exception e) {
			System.out.println("No se pueden extraer semillas");
			FILE_OUTPUT.escribir("\t No se pueden extraer semillas");
		} 		
	}
}
