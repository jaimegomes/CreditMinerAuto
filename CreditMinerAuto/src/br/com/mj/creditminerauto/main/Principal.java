package br.com.mj.creditminerauto.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import br.com.mj.creditminerauto.client.ClientWS;
import br.com.mj.creditminerauto.dto.ClienteDTO;
import br.com.mj.creditminerauto.dto.CsvDTO;
import br.com.mj.creditminerauto.model.Cliente;
import br.com.mj.creditminerauto.model.Contrato;
import br.com.mj.creditminerauto.util.Util;
import br.com.mj.creditminerauto.util.WriteFileCSV;

public class Principal {

	private static Thread worker;
	private static String ARQUIVO_LEITURA = "vanessa_bmg.csv";
	private static int contador;
	private static Logger log = Logger.getLogger("log");

	public static void main(String[] args) {
		PropertyConfigurator.configure("src/resources/log4j.properties");
		iniciarProcesso();

	}

	public static void iniciarProcesso() {

		log.info("Iniciando Processamento do arquivo: " + ARQUIVO_LEITURA);

		contador = 0;

		try {
			final List<CsvDTO> list = Util.parseCsvFileToBeans(CsvDTO.class, new File(getCaminhoArquivoLeitura()));

			worker = new Thread() {
				public void run() {

					List<ClienteDTO> listaClientesPreenchida = new ArrayList<ClienteDTO>();

					final int total = list.size();

					for (CsvDTO cpf : list) {

						long start = System.currentTimeMillis();

						String cpfFormatado = StringUtils.leftPad(cpf.getCpf(), 11, "0");

						List<Cliente> retornoJson = ClientWS.getInformacoesClienteWS(cpfFormatado);
						Cliente cliente = null;

						if (retornoJson != null && !retornoJson.isEmpty()) {

							cliente = retornoJson.get(0);

							// para cada contrato ele cria um objeto ClienteDTO
							// para
							// preencher o csv com todos os contratos.
							for (Contrato con : cliente.getResumoFinanceiro().getContratos()) {

								ClienteDTO clienteDTO = new ClienteDTO(cliente.getMatricula(), cliente.getNome(),
										cliente.getCpf(), cliente.getDataNascimento(), cliente.getIdade(),
										cliente.getSexo(), cliente.getOrgao(), cliente.getCargo(), cliente.getLotacao(),
										cliente.getSalario(), cliente.getRegimeJuridico(),
										cliente.getResumoFinanceiro().getDataCompetencia(),
										cliente.getResumoFinanceiro().getMargemConsignavelEmp(),
										cliente.getResumoFinanceiro().getValorConsignadoEmp(),
										cliente.getResumoFinanceiro().getMargemDisponivelEmp(),
										cliente.getResumoFinanceiro().getMargemConsignavelRmc(),
										cliente.getResumoFinanceiro().getValorConsignadoRmc(),
										cliente.getResumoFinanceiro().getMargemDisponivelRmc(),
										cliente.getResumoFinanceiro().getQtdEmp(),
										cliente.getResumoFinanceiro().getQtdRmc(), cliente.getTipo(),
										con.getIdContratoEmp(), con.getDataInicioDesconto(), con.getDataFimDesconto(),
										con.getIdBancoEmp(), con.getNomeBancoEmp(), con.getQtdParcelas(),
										con.getQtdParcelasRestante(), con.getValorQuitacao(),
										con.getValorRefinDisponivel(), con.getValorRefinBruto(), con.getValorParcela(),
										con.getTipoEmp());

								listaClientesPreenchida.add(clienteDTO);

							}

						}

						contador++;

						long end = System.currentTimeMillis();
						double totalTempoCpf = Util.calculaTempoExecucao(start, end);

						System.out.println(
								"Status " + contador + "/" + total + " tempo processamento: " + totalTempoCpf / 1000);
						log.info("Status " + contador + "/" + total + " tempo processamento: " + totalTempoCpf / 1000);
					}

					log.info(ARQUIVO_LEITURA + "Finalizado!!!");
					System.out.println(ARQUIVO_LEITURA + "Finalizado!!!");
					// stop na thread
					worker.interrupt();

					WriteFileCSV.createCsvFile(listaClientesPreenchida, new File(getCaminhoDestinoArquivo()));
				}
			};

			worker.start();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static String getCaminhoDestinoArquivo() {

		Date dataAtual = new Date();

		StringBuilder sbResultado = new StringBuilder();
		sbResultado.append("resultado");
		sbResultado.append("_");
		sbResultado.append(Util.retornaDia(dataAtual));
		sbResultado.append("_");
		sbResultado.append(Util.retornaMes(dataAtual));
		sbResultado.append("_");
		sbResultado.append(Util.retornaAno(dataAtual));
		sbResultado.append("_");
		sbResultado.append(ARQUIVO_LEITURA);

		return "/home/CreditMiner/arquivos_gerados" + File.separator + sbResultado.toString();

	}

	public static String getCaminhoArquivoLeitura() {
		return "/home/CreditMiner/leitura_automatica" + File.separator + ARQUIVO_LEITURA;

	}

}
