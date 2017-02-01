package br.com.mj.creditminerauto.dto;

public class ClienteDTO {

	//Cliente
	private String matricula;
	private String nome;
	private String cpf;
	private String dataNascimento;
	private String idade;
	private String sexo;
	private String orgao;
	private String cargo;
	private String lotacao;
	private String valorSalario;
	private String regimeJuridico;

	// Resumo Financeiro
	private String dataCompetencia;
	private String margemConsignavelEmp;
	private String valorConsignadoEmp;
	private String margemDisponivelEmp;
	private String margemConsignavelRmc;
	private String valorConsignadoRmc;
	private String margemDisponivelRmc;
	private String qtdEmp;
	private String qtdRmc;
	private String tipo;

	// Contratos
	private String idContratoEmp;
	private String dataInicioDesconto;
	private String dataFimDesconto;
	private String idBancoEmp;
	private String nomeBancoEmp;
	private String qtdParcelas;
	private String qtdParcelasRestante;
	private String valorQuitacao;
	private String valorRefinDisponivel;
	private String valorRefinBruto;
	private String valorParcela;
	private String tipoEmp;

	public ClienteDTO() {
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
	 * @param valorSalario
	 * @param regimeJuridico
	 * @param dataCompetencia
	 * @param margemConsignavelEmp
	 * @param valorConsignadoEmp
	 * @param margemDisponivelEmp
	 * @param margemConsignavelRmc
	 * @param valorConsignadoRmc
	 * @param margemDisponivelRmc
	 * @param qtdEmp
	 * @param qtdRmc
	 * @param tipo
	 * @param idContratoEmp
	 * @param dataInicioDesconto
	 * @param dataFimDesconto
	 * @param idBancoEmp
	 * @param nomeBancoEmp
	 * @param qtdParcelas
	 * @param qtdParcelasRestante
	 * @param valorQuitacao
	 * @param valorRefinDisponivel
	 * @param valorRefinBruto
	 * @param valorParcela
	 * @param tipoEmp
	 */
	public ClienteDTO(String matricula, String nome, String cpf, String dataNascimento, String idade, String sexo, String orgao, String cargo, String lotacao, String valorSalario,
			String regimeJuridico, String dataCompetencia, String margemConsignavelEmp, String valorConsignadoEmp, String margemDisponivelEmp, String margemConsignavelRmc, String valorConsignadoRmc,
			String margemDisponivelRmc, String qtdEmp, String qtdRmc, String tipo, String idContratoEmp, String dataInicioDesconto, String dataFimDesconto, String idBancoEmp, String nomeBancoEmp,
			String qtdParcelas, String qtdParcelasRestante, String valorQuitacao, String valorRefinDisponivel, String valorRefinBruto, String valorParcela, String tipoEmp) {
		this.matricula = matricula;
		this.nome = nome;
		this.cpf = cpf;
		this.dataNascimento = dataNascimento;
		this.idade = idade;
		this.sexo = sexo;
		this.orgao = orgao;
		this.cargo = cargo;
		this.lotacao = lotacao;
		this.valorSalario = valorSalario;
		this.regimeJuridico = regimeJuridico;
		this.dataCompetencia = dataCompetencia;
		this.margemConsignavelEmp = margemConsignavelEmp;
		this.valorConsignadoEmp = valorConsignadoEmp;
		this.margemDisponivelEmp = margemDisponivelEmp;
		this.margemConsignavelRmc = margemConsignavelRmc;
		this.valorConsignadoRmc = valorConsignadoRmc;
		this.margemDisponivelRmc = margemDisponivelRmc;
		this.qtdEmp = qtdEmp;
		this.qtdRmc = qtdRmc;
		this.tipo = tipo;
		this.idContratoEmp = idContratoEmp;
		this.dataInicioDesconto = dataInicioDesconto;
		this.dataFimDesconto = dataFimDesconto;
		this.idBancoEmp = idBancoEmp;
		this.nomeBancoEmp = nomeBancoEmp;
		this.qtdParcelas = qtdParcelas;
		this.qtdParcelasRestante = qtdParcelasRestante;
		this.valorQuitacao = valorQuitacao;
		this.valorRefinDisponivel = valorRefinDisponivel;
		this.valorRefinBruto = valorRefinBruto;
		this.valorParcela = valorParcela;
		this.tipoEmp = tipoEmp;
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
	 * @return the valorSalario
	 */
	public String getValorSalario() {
		return valorSalario;
	}

	/**
	 * @param valorSalario
	 *            the valorSalario to set
	 */
	public void setValorSalario(String valorSalario) {
		this.valorSalario = valorSalario;
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
	 * @return the dataCompetencia
	 */
	public String getDataCompetencia() {
		return dataCompetencia;
	}

	/**
	 * @param dataCompetencia
	 *            the dataCompetencia to set
	 */
	public void setDataCompetencia(String dataCompetencia) {
		this.dataCompetencia = dataCompetencia;
	}

	/**
	 * @return the margemConsignavelEmp
	 */
	public String getMargemConsignavelEmp() {
		return margemConsignavelEmp;
	}

	/**
	 * @param margemConsignavelEmp
	 *            the margemConsignavelEmp to set
	 */
	public void setMargemConsignavelEmp(String margemConsignavelEmp) {
		this.margemConsignavelEmp = margemConsignavelEmp;
	}

	/**
	 * @return the valorConsignadoEmp
	 */
	public String getValorConsignadoEmp() {
		return valorConsignadoEmp;
	}

	/**
	 * @param valorConsignadoEmp
	 *            the valorConsignadoEmp to set
	 */
	public void setValorConsignadoEmp(String valorConsignadoEmp) {
		this.valorConsignadoEmp = valorConsignadoEmp;
	}

	/**
	 * @return the margemDisponivelEmp
	 */
	public String getMargemDisponivelEmp() {
		return margemDisponivelEmp;
	}

	/**
	 * @param margemDisponivelEmp
	 *            the margemDisponivelEmp to set
	 */
	public void setMargemDisponivelEmp(String margemDisponivelEmp) {
		this.margemDisponivelEmp = margemDisponivelEmp;
	}

	/**
	 * @return the margemConsignavelRmc
	 */
	public String getMargemConsignavelRmc() {
		return margemConsignavelRmc;
	}

	/**
	 * @param margemConsignavelRmc
	 *            the margemConsignavelRmc to set
	 */
	public void setMargemConsignavelRmc(String margemConsignavelRmc) {
		this.margemConsignavelRmc = margemConsignavelRmc;
	}

	/**
	 * @return the valorConsignadoRmc
	 */
	public String getValorConsignadoRmc() {
		return valorConsignadoRmc;
	}

	/**
	 * @param valorConsignadoRmc
	 *            the valorConsignadoRmc to set
	 */
	public void setValorConsignadoRmc(String valorConsignadoRmc) {
		this.valorConsignadoRmc = valorConsignadoRmc;
	}

	/**
	 * @return the margemDisponivelRmc
	 */
	public String getMargemDisponivelRmc() {
		return margemDisponivelRmc;
	}

	/**
	 * @param margemDisponivelRmc
	 *            the margemDisponivelRmc to set
	 */
	public void setMargemDisponivelRmc(String margemDisponivelRmc) {
		this.margemDisponivelRmc = margemDisponivelRmc;
	}

	/**
	 * @return the qtdEmp
	 */
	public String getQtdEmp() {
		return qtdEmp;
	}

	/**
	 * @param qtdEmp
	 *            the qtdEmp to set
	 */
	public void setQtdEmp(String qtdEmp) {
		this.qtdEmp = qtdEmp;
	}

	/**
	 * @return the qtdRmc
	 */
	public String getQtdRmc() {
		return qtdRmc;
	}

	/**
	 * @param qtdRmc
	 *            the qtdRmc to set
	 */
	public void setQtdRmc(String qtdRmc) {
		this.qtdRmc = qtdRmc;
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

	/**
	 * @return the idContratoEmp
	 */
	public String getIdContratoEmp() {
		return idContratoEmp;
	}

	/**
	 * @param idContratoEmp
	 *            the idContratoEmp to set
	 */
	public void setIdContratoEmp(String idContratoEmp) {
		this.idContratoEmp = idContratoEmp;
	}

	/**
	 * @return the dataInicioDesconto
	 */
	public String getDataInicioDesconto() {
		return dataInicioDesconto;
	}

	/**
	 * @param dataInicioDesconto
	 *            the dataInicioDesconto to set
	 */
	public void setDataInicioDesconto(String dataInicioDesconto) {
		this.dataInicioDesconto = dataInicioDesconto;
	}

	/**
	 * @return the dataFimDesconto
	 */
	public String getDataFimDesconto() {
		return dataFimDesconto;
	}

	/**
	 * @param dataFimDesconto
	 *            the dataFimDesconto to set
	 */
	public void setDataFimDesconto(String dataFimDesconto) {
		this.dataFimDesconto = dataFimDesconto;
	}

	/**
	 * @return the idBancoEmp
	 */
	public String getIdBancoEmp() {
		return idBancoEmp;
	}

	/**
	 * @param idBancoEmp
	 *            the idBancoEmp to set
	 */
	public void setIdBancoEmp(String idBancoEmp) {
		this.idBancoEmp = idBancoEmp;
	}

	/**
	 * @return the nomeBancoEmp
	 */
	public String getNomeBancoEmp() {
		return nomeBancoEmp;
	}

	/**
	 * @param nomeBancoEmp
	 *            the nomeBancoEmp to set
	 */
	public void setNomeBancoEmp(String nomeBancoEmp) {
		this.nomeBancoEmp = nomeBancoEmp;
	}

	/**
	 * @return the qtdParcelas
	 */
	public String getQtdParcelas() {
		return qtdParcelas;
	}

	/**
	 * @param qtdParcelas
	 *            the qtdParcelas to set
	 */
	public void setQtdParcelas(String qtdParcelas) {
		this.qtdParcelas = qtdParcelas;
	}

	/**
	 * @return the qtdParcelasRestante
	 */
	public String getQtdParcelasRestante() {
		return qtdParcelasRestante;
	}

	/**
	 * @param qtdParcelasRestante
	 *            the qtdParcelasRestante to set
	 */
	public void setQtdParcelasRestante(String qtdParcelasRestante) {
		this.qtdParcelasRestante = qtdParcelasRestante;
	}

	/**
	 * @return the valorQuitacao
	 */
	public String getValorQuitacao() {
		return valorQuitacao;
	}

	/**
	 * @param valorQuitacao
	 *            the valorQuitacao to set
	 */
	public void setValorQuitacao(String valorQuitacao) {
		this.valorQuitacao = valorQuitacao;
	}

	/**
	 * @return the valorRefinDisponivel
	 */
	public String getValorRefinDisponivel() {
		return valorRefinDisponivel;
	}

	/**
	 * @param valorRefinDisponivel
	 *            the valorRefinDisponivel to set
	 */
	public void setValorRefinDisponivel(String valorRefinDisponivel) {
		this.valorRefinDisponivel = valorRefinDisponivel;
	}

	/**
	 * @return the valorRefinBruto
	 */
	public String getValorRefinBruto() {
		return valorRefinBruto;
	}

	/**
	 * @param valorRefinBruto
	 *            the valorRefinBruto to set
	 */
	public void setValorRefinBruto(String valorRefinBruto) {
		this.valorRefinBruto = valorRefinBruto;
	}

	/**
	 * @return the valorParcela
	 */
	public String getValorParcela() {
		return valorParcela;
	}

	/**
	 * @param valorParcela
	 *            the valorParcela to set
	 */
	public void setValorParcela(String valorParcela) {
		this.valorParcela = valorParcela;
	}

	/**
	 * @return the tipoEmp
	 */
	public String getTipoEmp() {
		return tipoEmp;
	}

	/**
	 * @param tipoEmp
	 *            the tipoEmp to set
	 */
	public void setTipoEmp(String tipoEmp) {
		this.tipoEmp = tipoEmp;
	}

}
