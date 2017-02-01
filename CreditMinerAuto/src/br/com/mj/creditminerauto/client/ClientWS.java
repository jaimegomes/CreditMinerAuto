package br.com.mj.creditminerauto.client;

import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

import br.com.mj.creditminerauto.model.Cliente;

public class ClientWS {

	public static List<Cliente> getInformacoesClienteWS(String cpf) {

		try {
			String urlws = "http://ws.consulta.plus/v2/govsc/cadastro/" + cpf
					+ "?apiKey=XlYYdir53yerhDz5sgkgktyudgoLF4o7vde4";
			

			Client c = Client.create();
			
			WebResource wr = c.resource(urlws);

			String json = wr.get(String.class);

			Gson gson = new Gson();
			
			return gson.fromJson(json, new TypeToken<List<Cliente>>() {
			}.getType());
			
		} catch (Exception e) {
			System.out.println("CPF: " + cpf + " " + e.getMessage());
			
			if (e.getMessage().indexOf("Gateway") > 0) {
				System.out.println("TRATANDO ERRO, REPETINDO CPF " + cpf);
				getInformacoesClienteWS(cpf);
			} 
		}
		return null;

	}

}
