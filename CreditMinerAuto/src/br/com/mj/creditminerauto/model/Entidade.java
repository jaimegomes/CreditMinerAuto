package br.com.mj.creditminerauto.model;

import java.io.Serializable;

/**
 * Classe abstract Entidade modelo para criação de entidades
 * 
 * @author Marcelo Lopes Nunes</br>
 *         bjjsolutions.com.br - 08/06/2016</br>
 *         <a href=malito:lopesnunnes@gmail.com>lopesnunnes@gmail.com</a>
 * 
 */
public abstract class Entidade implements Serializable {

	private static final long serialVersionUID = 1L;

	public abstract void setId(Long id);

	public abstract Long getId();
}