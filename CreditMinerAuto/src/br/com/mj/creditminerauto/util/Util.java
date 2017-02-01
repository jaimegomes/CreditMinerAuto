package br.com.mj.creditminerauto.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameTranslateMappingStrategy;

/**
 * Classe Util para criação de métodos utilitarios do sistema
 * 
 * @author Marcelo Lopes Nunes</br> bjjsolutions.com.br - 30/05/2016</br> <a
 *         href=malito:lopesnunnes@gmail.com>lopesnunnes@gmail.com</a>
 * 
 */
public class Util {

	private static Locale localePtBR = null;

	public static String getProperty(String key) {
		return getResourceBundle().getString(key);
	}

	private static ResourceBundle getResourceBundle() {
		ResourceBundle resourceBundle = ResourceBundle.getBundle("resources.messageJSF", getLocalePtBR());
		return resourceBundle;
	}

	private static Locale getLocalePtBR() {
		if (localePtBR == null) {
			localePtBR = new Locale("pt", "BR");
		}
		return localePtBR;
	}

	/**
	 * Método que transforma cada cpf do arquivo csv em um objeto CsvDTO, nele
	 * contem o cpf utilizado para as pesquisas
	 * 
	 * @param beanClass
	 * @return List<CsvDTO>
	 * @throws IOException
	 * 
	 * @TODO FAZER VALIDAÇÃO DO NOME DO ARQUIVO (SE CONTEM .CSV), VALIDAR SE O
	 *       CABEÇALHO DO ARQUIVO TEM A PALAVRA CPF
	 */
	@SuppressWarnings("deprecation")
	public static <CsvDTO> List<CsvDTO> parseCsvFileToBeans(final Class<CsvDTO> beanClass, File file) throws IOException {
		CSVReader reader = null;

		try {
			reader = new CSVReader(new BufferedReader(new FileReader(file.getAbsolutePath())), ';');

			Map<String, String> columnMapping = new HashMap<String, String>();
			columnMapping.put("CPF", "cpf");

			HeaderColumnNameTranslateMappingStrategy<CsvDTO> strategy = new HeaderColumnNameTranslateMappingStrategy<CsvDTO>();
			strategy.setType(beanClass);
			strategy.setColumnMapping(columnMapping);

			final CsvToBean<CsvDTO> csv = new CsvToBean<CsvDTO>();

			return csv.parse(strategy, reader);

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (final IOException e) {
					e.printStackTrace();
				}
			}
		}

		return null;

	}

	/**
	 * Método que calcula o tempo de execução em mili segundos
	 * 
	 * @param start
	 * @param end
	 * @return tempo total de execução
	 */
	public static long calculaTempoExecucao(long start, long end) {
		return (end - start);

	}

	public static int retornaDia(Date data) {
		Calendar c = new GregorianCalendar();
		c.setTime(data);
		return c.get(Calendar.DAY_OF_MONTH);
	}

	public static int retornaMes(Date data) {
		Calendar c = new GregorianCalendar();
		c.setTime(data);
		return c.get(Calendar.MONTH);
	}

	public static int retornaAno(Date data) {
		Calendar c = new GregorianCalendar();
		c.setTime(data);
		return c.get(Calendar.YEAR);
	}
}