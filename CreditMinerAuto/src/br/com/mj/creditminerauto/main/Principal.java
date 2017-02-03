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
	private static String DIRETORIO = "/home/CreditMiner/leitura_automatica";
	private static Logger log = Logger.getLogger("log");

	public static void main(String[] args) {
		PropertyConfigurator.configure("src/resources/log4j.properties");

		File diretorio = new File(DIRETORIO);
		File listArquivos[] = diretorio.listFiles();

		for (File arquivo : listArquivos) {
			if (arquivo.getName().contains(".csv")) {
				iniciarProcesso(arquivo);
			}
		}

	}

	public static void iniciarProcesso(final File file) {

		log.info("Iniciando Processamento do arquivo: " + file.getName());

		try {
			final List<CsvDTO> list = Util.parseCsvFileToBeans(CsvDTO.class, new File(file.getAbsolutePath()));

			worker = new Thread() {
				public void run() {

					List<ClienteDTO> listaClientesPreenchida = new ArrayList<ClienteDTO>();
					ClienteDTO clienteDTO = null;

					for (CsvDTO cpf : list) {

						String cpfFormatado = StringUtils.leftPad(cpf.getCpf(), 11, "0");

						List<Cliente> retornoJson = ClientWS.getInformacoesClienteWS(cpfFormatado);
						Cliente cliente = null;

						if (retornoJson != null && !retornoJson.isEmpty()) {

							cliente = retornoJson.get(0);
							List<Contrato> listContratos = cliente.getResumoFinanceiro().getContratos();

							// verifica se existe contrato, caso não exista ele
							// cria um objeto somente com os dados existentes
							if (listContratos == null || listContratos.isEmpty()) {

								clienteDTO = new ClienteDTO(cliente.getMatricula(), cliente.getNome(), cliente.getCpf(),
										cliente.getDataNascimento(), cliente.getIdade(), cliente.getSexo(),
										cliente.getOrgao(), cliente.getCargo(), cliente.getLotacao(),
										cliente.getSalario(), cliente.getRegimeJuridico(),
										cliente.getResumoFinanceiro().getDataCompetencia(),
										cliente.getResumoFinanceiro().getMargemConsignavelEmp(),
										cliente.getResumoFinanceiro().getValorConsignadoEmp(),
										cliente.getResumoFinanceiro().getMargemDisponivelEmp(),
										cliente.getResumoFinanceiro().getMargemConsignavelRmc(),
										cliente.getResumoFinanceiro().getValorConsignadoRmc(),
										cliente.getResumoFinanceiro().getMargemDisponivelRmc(),
										cliente.getResumoFinanceiro().getQtdEmp(),
										cliente.getResumoFinanceiro().getQtdRmc(), cliente.getTipo(), null, null, null,
										null, null, null, null, null, null, null, null, null);

								listaClientesPreenchida.add(clienteDTO);

							} else {

								// caso exista contrato ele cria um objeto para
								// cada contrato
								for (Contrato con : cliente.getResumoFinanceiro().getContratos()) {

									clienteDTO = new ClienteDTO(cliente.getMatricula(), cliente.getNome(),
											cliente.getCpf(), cliente.getDataNascimento(), cliente.getIdade(),
											cliente.getSexo(), cliente.getOrgao(), cliente.getCargo(),
											cliente.getLotacao(), cliente.getSalario(), cliente.getRegimeJuridico(),
											cliente.getResumoFinanceiro().getDataCompetencia(),
											cliente.getResumoFinanceiro().getMargemConsignavelEmp(),
											cliente.getResumoFinanceiro().getValorConsignadoEmp(),
											cliente.getResumoFinanceiro().getMargemDisponivelEmp(),
											cliente.getResumoFinanceiro().getMargemConsignavelRmc(),
											cliente.getResumoFinanceiro().getValorConsignadoRmc(),
											cliente.getResumoFinanceiro().getMargemDisponivelRmc(),
											cliente.getResumoFinanceiro().getQtdEmp(),
											cliente.getResumoFinanceiro().getQtdRmc(), cliente.getTipo(),
											con.getIdContratoEmp(), con.getDataInicioDesconto(),
											con.getDataFimDesconto(), con.getIdBancoEmp(), con.getNomeBancoEmp(),
											con.getQtdParcelas(), con.getQtdParcelasRestante(), con.getValorQuitacao(),
											con.getValorRefinDisponivel(), con.getValorRefinBruto(),
											con.getValorParcela(), con.getTipoEmp());

									listaClientesPreenchida.add(clienteDTO);

								}
							}

						}

					}

					log.info(file.getName() + " Finalizado!!!");
					System.out.println(file.getName() + " Finalizado!!!");
					// stop na thread
					worker.interrupt();

					WriteFileCSV.createCsvFile(listaClientesPreenchida,
							new File(getCaminhoDestinoArquivo(file.getName())));
				}
			};

			worker.start();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static String getCaminhoDestinoArquivo(String nomeArquivo) {

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
		sbResultado.append(nomeArquivo);

		return "/home/CreditMiner/arquivos_gerados" + File.separator + sbResultado.toString();

	}

}
