package br.com.mj.creditminerauto.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import br.com.mj.creditminerauto.dto.ClienteDTO;

/**
 * Classe que gera o csv com os dados processados
 * 
 * @author Marcelo Lopes Nunes</br>
 *         bjjsolutions.com.br - 30/06/2016</br>
 *         <a href=malito:lopesnunnes@gmail.com>lopesnunnes@gmail.com</a>
 * 
 */
public class WriteFileCSV {

	private static final String COMMA_DELIMITER = ";";
	private static final String NEW_LINE_SEPARATOR = "\n";
	private static final String FILE_HEADER = "MATRICULA;NOME;CPF;DATA_NASCIMENTO;IDADE;SEXO;ORGÃO;CARGO;LOTAÇÃO;VALOR_SALÁRIO;REGIME_JURIDICO;DATA_COMPETÊNCIA;MARGEM_CONSIGNAVEL_EMP;VALOR_CONSIGNADO_EMP;MARGEM_DISPONIVEL_EMP;MARGEM_CONSIGNAVEL_RMC;VALOR_CONSIGNADO_RMC;MARGEM_DISPONIVEL_RMC;QUANTIDADE_EMP;QUANTIDADE_RMC;ID_CONTRATO_EMP;DT_INI_DESCONTO;DT_FIM_DESCONTO;ID_BANCO_EMP;NOME_BANCO_EMP;QUANTIDADE_PARCELAS;QUANTIDADE_PARCELAS_REST;VALOR_QUITAÇÃO;VALOR_REFIN_DISP;VALOR_REFIN_BRUTO;VALOR_PARCELA;TIPO_EMP;TIPO";
	private static Logger log = Logger.getLogger("log");

	public static void createCsvFile(List<ClienteDTO> clientesDTO, File destino) {

		PropertyConfigurator.configure("src/resources/log4j.properties");

		log.info("Gerando arquivo csv " + destino.getAbsolutePath());

		FileWriter fileWriter = null;

		try {
			fileWriter = new FileWriter(destino.getAbsolutePath(), true);

			fileWriter.append(FILE_HEADER.toString());

			fileWriter.append(NEW_LINE_SEPARATOR);

			for (ClienteDTO dto : clientesDTO) {

				writeLine(fileWriter, dto);
			}

			log.info("Arquivo CSV criado com sucesso. " + destino.getAbsolutePath());
			System.out.println("Arquivo CSV criado com sucesso");

		} catch (Exception e) {
			System.err.println("Erro na criação do arquivo");
			log.error("Erro na criação do arquivo");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				System.err.println("Error while flushing/closing fileWriter !!!");
				log.error("Error while flushing/closing fileWriter !!!");
				e.printStackTrace();
			}
		}
	}

	private static void writeLine(FileWriter fileWriter, ClienteDTO clienteDTO) throws IOException {

		// Cliente
		fileWriter.append(clienteDTO.getMatricula());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getNome());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getCpf());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getDataNascimento());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getIdade());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getSexo());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getOrgao());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getCargo());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getLotacao());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getValorSalario());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getRegimeJuridico());
		fileWriter.append(COMMA_DELIMITER);

		// Resumo Financeiro
		fileWriter.append(clienteDTO.getDataCompetencia() != null ? clienteDTO.getDataCompetencia() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getMargemConsignavelEmp() != null ? clienteDTO.getMargemConsignavelEmp() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getValorConsignadoEmp() != null ? clienteDTO.getValorConsignadoEmp() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getMargemDisponivelEmp() != null ? clienteDTO.getMargemDisponivelEmp() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getMargemConsignavelRmc() != null ? clienteDTO.getMargemConsignavelRmc() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getValorConsignadoRmc() != null ? clienteDTO.getValorConsignadoRmc() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getMargemDisponivelRmc() != null ? clienteDTO.getMargemDisponivelRmc() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getQtdEmp() != null ? clienteDTO.getQtdEmp() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getQtdRmc() != null ? clienteDTO.getQtdRmc() : "");
		fileWriter.append(COMMA_DELIMITER);

		// Contratos
		fileWriter.append(clienteDTO.getIdContratoEmp() != null ? clienteDTO.getIdContratoEmp() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getDataInicioDesconto() != null ? clienteDTO.getDataInicioDesconto() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getDataFimDesconto() != null ? clienteDTO.getDataFimDesconto() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getIdBancoEmp() != null ? clienteDTO.getIdBancoEmp() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getNomeBancoEmp() != null ? clienteDTO.getNomeBancoEmp() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getQtdParcelas() != null ? clienteDTO.getQtdParcelas() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getQtdParcelasRestante() != null ? clienteDTO.getQtdParcelasRestante() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getValorQuitacao() != null ? clienteDTO.getValorQuitacao() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getValorRefinDisponivel() != null ? clienteDTO.getValorRefinDisponivel() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getValorRefinBruto() != null ? clienteDTO.getValorRefinBruto() : "");
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getValorParcela());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getTipoEmp());
		fileWriter.append(COMMA_DELIMITER);
		fileWriter.append(clienteDTO.getTipo());
		fileWriter.append(NEW_LINE_SEPARATOR);

	}
}
