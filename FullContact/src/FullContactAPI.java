
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.fullcontact.api.libs.fullcontact4j.FullContact;
import com.fullcontact.api.libs.fullcontact4j.FullContactException;
import com.fullcontact.api.libs.fullcontact4j.enums.Casing;
import com.fullcontact.api.libs.fullcontact4j.http.name.NameDeduceRequest;
import com.fullcontact.api.libs.fullcontact4j.http.name.NameResponse;
import com.fullcontact.api.libs.fullcontact4j.http.person.PersonRequest;
import com.fullcontact.api.libs.fullcontact4j.http.person.PersonResponse;
import com.fullcontact.api.libs.fullcontact4j.http.person.model.SocialProfile;
import com.google.gson.Gson;


public class FullContactAPI {

	private FullContact fullcontact;
	private BufferedReader contacts;
	private Client client;
	private BulkProcessor bulk;
	private Settings settings;
	private HashMap<PersonResponse,List<SocialProfile>> persons= new HashMap<PersonResponse,List<SocialProfile>>();
	private int sucess=0;
	
	public FullContactAPI(){
		init();
	}
	
	
	private void init() {
		// TODO Auto-generated method stub
		fullcontact = FullContact.withApiKey("8b08232a26e32386").setUserAgent("FC Sunset Services 1.0").build();
		settings = Settings.settingsBuilder().put("cluster.name","Logstash_Sunset").build();
		try {
			client = new TransportClient.Builder().settings(settings).build()
					.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("10.194.25.220"),9300));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		bulk = BulkProcessor.builder(client, new Listener() {
			
			@Override
			public void beforeBulk(long arg0, BulkRequest arg1) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {
				// TODO Auto-generated method stub
				
			}
		})
		.setBulkActions(3)
		.setConcurrentRequests(1)
		.build();
		
	}

	
	public FullContactAPI(String filename){
		init();
		//Read Emails
		try {
			contacts = new BufferedReader(new FileReader(filename));
			String line;
			while( (line= contacts.readLine()) != null){
				Member m = new Member();
				StringTokenizer tokens = new StringTokenizer(line,"|");
				
				m.setContract(tokens.nextToken());
				m.setName_contract(tokens.nextToken());
				m.setEmail(tokens.nextToken());
				
				//buildName(email);
				buildPerson(m);
				try {
					System.out.println("Waiting....");
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		
	}
	
	private void buildPerson(Member m) {
		// TODO Auto-generated method stub
		PersonRequest request = fullcontact.buildPersonRequest().email(m.getEmail()).build();
		PersonResponse presponse;
		try {
			presponse = fullcontact.sendRequest(request);
			if(presponse.getStatus()==200){
				System.out.println("Sucesss");
				sucess++;
				//persons.put(presponse, presponse.getSocialProfiles());
				//Iterator<SocialProfile> socialprofiles = presponse.getSocialProfiles().iterator();
				//while(socialprofiles.hasNext()){
				//	System.out.println(socialprofiles.next().toString());
				//}
				m.setData(presponse);
				Gson gson = new Gson();
				String json = gson.toJson(m);
				System.out.println(json);
				if(sucess>490){
					System.out.println("limit reached ....");
					return;
				}
					
				IndexRequest index  = new IndexRequest("fullcontact", "Person").source(json);
				bulk.add(index);
						
						//client.prepareIndex("fullcontact", "Person").setSource(persons);
				//bulk.add(new IndexRequest("fullcontact","Person").set );
				
				//System.out.println(sp.getTypeName());
				//System.out.println(presponse.getContactInfo().toString());
				
				//System.out.println(presponse.getDemographics().toString());			
				
			}
			
		} catch (FullContactException e) {
			// TODO Auto-generated catch block
			System.out.println("Not Found:" + m.getEmail() + " contract: " + m.getContract());
		}
		
		
		
		
	}

	private void buildName(String email) {
		// TODO Auto-generated method stub
		NameDeduceRequest nd = fullcontact.buildNameDeduceRequest().email(email).casing(Casing.TITLECASE).build();
		NameResponse response;
		try {
			response = fullcontact.sendRequest(nd);
			String name = response.getNameDetails().getFullName();
			//System.out.println(name);
			System.out.println(response.getNameDetails().getFullName());
			
		} catch (FullContactException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	public static void main(String[] args) {
	
		//FullContactAPI api = new FullContactAPI("/Users/EdgarOsorio/Documents/emails.txt");
		FullContactAPI api = new FullContactAPI();
		//String contract="1--1--3268";
		String contract="";
		String full_name="ANGELA K. WILLIAMS";
		String email = "";
		ArrayList<Member> m = api.search(email,contract,full_name);
		
		//ArrayList<Member> members = api.getAllData();
		
		
		
	}

	private ArrayList<Member> search(String email, String contract, String full_name) {
		
		//Search in elasticsearch
		Gson gson = new Gson();
		ArrayList<Member> members = new ArrayList<Member>();
		SearchResponse response = client.prepareSearch("fullcontact").setTypes("Person").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				//.setQuery(QueryBuilders.matchQuery("email",email))
				.setQuery(QueryBuilders.multiMatchQuery(email+" "+ contract+" " + full_name , "email","contract","name_contract"))
				.setExplain(true)
				.execute().actionGet();
		
		Iterator<SearchHit> hits =  response.getHits().iterator();
		while(hits.hasNext()){
			 SearchHit h = hits.next();
			 //System.out.println(h.toString());
			 Member m =  gson.fromJson(h.getSourceAsString(),Member.class);
			 members.add(m);
			 System.out.println(m.toString());

		}// TODO Auto-generated method stub
		return members;
	}
	
	
}
