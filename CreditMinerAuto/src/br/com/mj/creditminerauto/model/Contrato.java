package br.com.mj.creditminerauto.model;

import com.google.gson.annotations.SerializedName;

public class Contrato {
	
	@SerializedName("ID_CONTRATO_EMP")
	private String idContratoEmp;
	
	@SerializedName("DT_INI_DESCONTO")
	private String dataInicioDesconto;
	
	@SerializedName("DT_FIM_DESCONTO")
	private String dataFimDesconto;
	
	@SerializedName("ID_BANCO_EMP")
	private String idBancoEmp;
	
	@SerializedName("NOME_BANCO_EMP")
	private String nomeBancoEmp;
	
	@SerializedName("QUANTIDADE_PARCELAS")
	private String qtdParcelas;
	
	@SerializedName("QUANTIDADE_PARCELAS_REST")
	private String qtdParcelasRestante;
	
	@SerializedName("VALOR_QUITACAO")
	private String valorQuitacao;
	
	@SerializedName("VALOR_REFIN_DISP")
	private String valorRefinDisponivel;
	
	@SerializedName("VALOR_REFIN_BRUTO")
	private String valorRefinBruto;
	
	@SerializedName("VALOR_PARCELA")
	private String valorParcela;
	
	@SerializedName("TIPO_EMP")
	private String tipoEmp;

	public Contrato() {
	}

	/**
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
	public Contrato(String idContratoEmp, String dataInicioDesconto, String dataFimDesconto, String idBancoEmp, String nomeBancoEmp, String qtdParcelas, String qtdParcelasRestante,
			String valorQuitacao, String valorRefinDisponivel, String valorRefinBruto, String valorParcela, String tipoEmp) {
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
