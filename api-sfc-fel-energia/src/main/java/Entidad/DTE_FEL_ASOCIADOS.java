package Entidad;

import java.io.Serializable;

public class DTE_FEL_ASOCIADOS implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private Long id_dte;
    private Integer tipo_registro;
    private String id_tipo_documento;
    private String serie;
    private Long numero;
    private Long fecha_documento;

    public DTE_FEL_ASOCIADOS(Long id_dte, Integer tipo_registro, String id_tipo_documento, String serie, Long numero, Long fecha_documento) {
        this.id_dte = id_dte;
        this.tipo_registro = tipo_registro;
        this.id_tipo_documento = id_tipo_documento;
        this.serie = serie;
        this.numero = numero;
        this.fecha_documento = fecha_documento;
    }

    public Long getId_dte() {
        return id_dte;
    }

    public void setId_dte(Long id_dte) {
        this.id_dte = id_dte;
    }

    public Integer getTipo_registro() {
        return tipo_registro;
    }

    public void setTipo_registro(Integer tipo_registro) {
        this.tipo_registro = tipo_registro;
    }

    public String getId_tipo_documento() {
        return id_tipo_documento;
    }

    public void setId_tipo_documento(String id_tipo_documento) {
        this.id_tipo_documento = id_tipo_documento;
    }

    public String getSerie() {
        return serie;
    }

    public void setSerie(String serie) {
        this.serie = serie;
    }

    public Long getNumero() {
        return numero;
    }

    public void setNumero(Long numero) {
        this.numero = numero;
    }

    public Long getFecha_documento() {
        return fecha_documento;
    }

    public void setFecha_documento(Long fecha_documento) {
        this.fecha_documento = fecha_documento;
    }
    
}
