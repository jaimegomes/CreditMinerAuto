package br.com.mj.creditminerauto.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class ResumoFinanceiro {
	@SerializedName("DT_COMPETENCIA")
	private String dataCompetencia;

	@SerializedName("MARGEM_CONSIGNAVEL_EMP")
	private String margemConsignavelEmp;

	@SerializedName("VALOR_CONSIGNADO_EMP")
	private String valorConsignadoEmp;

	@SerializedName("MARGEM_DISPONIVEL_EMP")
	private String margemDisponivelEmp;

	@SerializedName("MARGEM_CONSIGNAVEL_RMC")
	private String margemConsignavelRmc;

	@SerializedName("VALOR_CONSIGNADO_RMC")
	private String valorConsignadoRmc;

	@SerializedName("MARGEM_DISPONIVEL_RMC")
	private String margemDisponivelRmc;

	@SerializedName("QUANTIDADE_EMP")
	private String qtdEmp;

	@SerializedName("QUANTIDADE_RMC")
	private String qtdRmc;

	@SerializedName("CONTRATOS")
	private List<Contrato> contratos;

	public ResumoFinanceiro() {
	}

	/**
	 * @param dataCompetencia
	 * @param margemConsignavelEmp
	 * @param valorConsignadoEmp
	 * @param margemDisponivelEmp
	 * @param margemConsignavelRmc
	 * @param valorConsignadoRmc
	 * @param margemDisponivelRmc
	 * @param qtdEmp
	 * @param qtdRmc
	 * @param contratos
	 * @param tipo
	 */
	public ResumoFinanceiro(String dataCompetencia, String margemConsignavelEmp, String valorConsignadoEmp, String margemDisponivelEmp, String margemConsignavelRmc, String valorConsignadoRmc,
			String margemDisponivelRmc, String qtdEmp, String qtdRmc, List<Contrato> contratos) {
		this.dataCompetencia = dataCompetencia;
		this.margemConsignavelEmp = margemConsignavelEmp;
		this.valorConsignadoEmp = valorConsignadoEmp;
		this.margemDisponivelEmp = margemDisponivelEmp;
		this.margemConsignavelRmc = margemConsignavelRmc;
		this.valorConsignadoRmc = valorConsignadoRmc;
		this.margemDisponivelRmc = margemDisponivelRmc;
		this.qtdEmp = qtdEmp;
		this.qtdRmc = qtdRmc;
		this.contratos = contratos;
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
	 * @return the contratos
	 */
	public List<Contrato> getContratos() {
		return contratos;
	}

	/**
	 * @param contratos
	 *            the contratos to set
	 */
	public void setContratos(List<Contrato> contratos) {
		this.contratos = contratos;
	}


}
