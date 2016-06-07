import com.fullcontact.api.libs.fullcontact4j.http.person.PersonResponse;

public class Member {

	private String email;
	private String name_contract;
	private String contract;
	private PersonResponse data;
	
	public String getEmail() {
		return email;
	}
	@Override
	public String toString() {
		return "Member [email=" + email + ", name_contract=" + name_contract + ", contract=" + contract + ", data="
				+ data.toString() + "]";
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getName_contract() {
		return name_contract;
	}
	public void setName_contract(String name_contract) {
		this.name_contract = name_contract;
	}
	public String getContract() {
		return contract;
	}
	public void setContract(String contract) {
		this.contract = contract;
	}
	public PersonResponse getData() {
		return data;
	}
	public void setData(PersonResponse data) {
		this.data = data;
	}
	
	
	
}
