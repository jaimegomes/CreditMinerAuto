package br.com.mj.creditminerauto.model;

import com.google.gson.annotations.SerializedName;

public class Cliente {

	@SerializedName("MATRICULA")
	private String matricula;

	@SerializedName("NOME")
	private String nome;

	@SerializedName("CPF")
	private String cpf;

	@SerializedName("DT_NASCIMENTO")
	private String dataNascimento;

	@SerializedName("IDADE")
	private String idade;

	@SerializedName("SEXO")
	private String sexo;

	@SerializedName("ORGAO")
	private String orgao;

	@SerializedName("CARGO")
	private String cargo;

	@SerializedName("LOTACAO")
	private String lotacao;

	@SerializedName("VALOR_SALARIO")
	private String salario;

	@SerializedName("REGIME_JURIDICO")
	private String regimeJuridico;

	@SerializedName("RESUMO_FINANCEIRO")
	private ResumoFinanceiro resumoFinanceiro;

	@SerializedName("TIPO")
	private String tipo;

	public Cliente() {
	}
	/**
	 * @param matricula
	 * @param nome
	 * @param cpf
	 * @param dataNascimento
	 * @param idade
	 * @param sexo
	 * @param orgao
	 * @param cargo
	 * @param lotacao
	 * @param salario
	 * @param regimeJuridico
	 * @param resumoFinanceiro
	 * @param tipo
	 */
	public Cliente(String matricula, String nome, String cpf, String dataNascimento, String idade, String sexo, String orgao, String cargo, String lotacao, String salario, String regimeJuridico,
			ResumoFinanceiro resumoFinanceiro, String tipo) {
		this.matricula = matricula;
		this.nome = nome;
		this.cpf = cpf;
		this.dataNascimento = dataNascimento;
		this.idade = idade;
		this.sexo = sexo;
		this.orgao = orgao;
		this.cargo = cargo;
		this.lotacao = lotacao;
		this.salario = salario;
		this.regimeJuridico = regimeJuridico;
		this.resumoFinanceiro = resumoFinanceiro;
		this.tipo = tipo;
	}
	/**
	 * @return the matricula
	 */
	public String getMatricula() {
		return matricula;
	}
	/**
	 * @param matricula
	 *            the matricula to set
	 */
	public void setMatricula(String matricula) {
		this.matricula = matricula;
	}
	/**
	 * @return the nome
	 */
	public String getNome() {
		return nome;
	}
	/**
	 * @param nome
	 *            the nome to set
	 */
	public void setNome(String nome) {
		this.nome = nome;
	}
	/**
	 * @return the cpf
	 */
	public String getCpf() {
		return cpf;
	}
	/**
	 * @param cpf
	 *            the cpf to set
	 */
	public void setCpf(String cpf) {
		this.cpf = cpf;
	}
	/**
	 * @return the dataNascimento
	 */
	public String getDataNascimento() {
		return dataNascimento;
	}
	/**
	 * @param dataNascimento
	 *            the dataNascimento to set
	 */
	public void setDataNascimento(String dataNascimento) {
		this.dataNascimento = dataNascimento;
	}
	/**
	 * @return the idade
	 */
	public String getIdade() {
		return idade;
	}
	/**
	 * @param idade
	 *            the idade to set
	 */
	public void setIdade(String idade) {
		this.idade = idade;
	}
	/**
	 * @return the sexo
	 */
	public String getSexo() {
		return sexo;
	}
	/**
	 * @param sexo
	 *            the sexo to set
	 */
	public void setSexo(String sexo) {
		this.sexo = sexo;
	}
	/**
	 * @return the orgao
	 */
	public String getOrgao() {
		return orgao;
	}
	/**
	 * @param orgao
	 *            the orgao to set
	 */
	public void setOrgao(String orgao) {
		this.orgao = orgao;
	}
	/**
	 * @return the cargo
	 */
	public String getCargo() {
		return cargo;
	}
	/**
	 * @param cargo
	 *            the cargo to set
	 */
	public void setCargo(String cargo) {
		this.cargo = cargo;
	}
	/**
	 * @return the lotacao
	 */
	public String getLotacao() {
		return lotacao;
	}
	/**
	 * @param lotacao
	 *            the lotacao to set
	 */
	public void setLotacao(String lotacao) {
		this.lotacao = lotacao;
	}
	/**
	 * @return the salario
	 */
	public String getSalario() {
		return salario;
	}
	/**
	 * @param salario
	 *            the salario to set
	 */
	public void setSalario(String salario) {
		this.salario = salario;
	}
	/**
	 * @return the regimeJuridico
	 */
	public String getRegimeJuridico() {
		return regimeJuridico;
	}
	/**
	 * @param regimeJuridico
	 *            the regimeJuridico to set
	 */
	public void setRegimeJuridico(String regimeJuridico) {
		this.regimeJuridico = regimeJuridico;
	}
	/**
	 * @return the resumoFinanceiro
	 */
	public ResumoFinanceiro getResumoFinanceiro() {
		return resumoFinanceiro;
	}
	/**
	 * @param resumoFinanceiro
	 *            the resumoFinanceiro to set
	 */
	public void setResumoFinanceiro(ResumoFinanceiro resumoFinanceiro) {
		this.resumoFinanceiro = resumoFinanceiro;
	}
	/**
	 * @return the tipo
	 */
	public String getTipo() {
		return tipo;
	}
	/**
	 * @param tipo
	 *            the tipo to set
	 */
	public void setTipo(String tipo) {
		this.tipo = tipo;
	}

}
