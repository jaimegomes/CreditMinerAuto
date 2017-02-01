package br.com.mj.creditminerauto.model;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

/**
 * Classe que representa a entidade Usuario utilizada para se logar no sistema
 * Credit Miner / Consignum
 * 
 * @author Jaime Gomes
 * 
 */
@Entity
@Table(name = "usuario")
@NamedQueries({ @NamedQuery(name = "Usuario.findAll", query = "SELECT u FROM Usuario u") })
public class Usuario extends Entidade implements Serializable {

	private static final long serialVersionUID = 1L;
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name = "id", nullable = false, unique = true)
	private Long id;
	@Column(name = "login", nullable = false, unique = true)
	private String login;
	@Column(name = "senha", nullable = false)
	private String senha;
	@Column(name = "banco", nullable = false)
	private String banco;

	/**
	 * Contrutor
	 */
	public Usuario() {

	}

	public Usuario(Long id, String login, String senha, String banco) {
		super();
		this.id = id;
		this.login = login;
		this.senha = senha;
		this.banco = banco;
	}

	/**
	 * @return the id
	 */
	public Long getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(Long id) {
		this.id = id;
	}

	/**
	 * @return the login
	 */
	public String getLogin() {
		return login;
	}

	/**
	 * @param login
	 *            the login to set
	 */
	public void setLogin(String login) {
		this.login = login;
	}

	/**
	 * @return the senha
	 */
	public String getSenha() {
		return senha;
	}

	/**
	 * @param senha
	 *            the senha to set
	 */
	public void setSenha(String senha) {
		this.senha = senha;
	}

	/**
	 * @return the banco
	 */
	public String getBanco() {
		return banco;
	}

	/**
	 * @param banco
	 *            the banco to set
	 */
	public void setBanco(String banco) {
		this.banco = banco;
	}

	@Override
	public String toString() {

		return this.banco;
	}
}
