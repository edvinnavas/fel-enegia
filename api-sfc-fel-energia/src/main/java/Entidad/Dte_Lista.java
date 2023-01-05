package Entidad;

import java.io.Serializable;
import java.util.Date;

public class Dte_Lista implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private Long id_dte;
    private Integer ab_cliente;
    private String cliente;
    private Integer no_orden;
    private String tipo_orden;
    private Integer no_documento;
    private Date fecha_documento;
    private String enviado;
    private String observacion;

    public Dte_Lista(Long id_dte, Integer ab_cliente, String cliente, Integer no_orden, String tipo_orden, Integer no_documento, Date fecha_documento, String enviado, String observacion) {
        this.id_dte = id_dte;
        this.ab_cliente = ab_cliente;
        this.cliente = cliente;
        this.no_orden = no_orden;
        this.tipo_orden = tipo_orden;
        this.no_documento = no_documento;
        this.fecha_documento = fecha_documento;
        this.enviado = enviado;
        this.observacion = observacion;
    }

    public Long getId_dte() {
        return id_dte;
    }

    public void setId_dte(Long id_dte) {
        this.id_dte = id_dte;
    }

    public Integer getAb_cliente() {
        return ab_cliente;
    }

    public void setAb_cliente(Integer ab_cliente) {
        this.ab_cliente = ab_cliente;
    }

    public String getCliente() {
        return cliente;
    }

    public void setCliente(String cliente) {
        this.cliente = cliente;
    }

    public Integer getNo_orden() {
        return no_orden;
    }

    public void setNo_orden(Integer no_orden) {
        this.no_orden = no_orden;
    }

    public String getTipo_orden() {
        return tipo_orden;
    }

    public void setTipo_orden(String tipo_orden) {
        this.tipo_orden = tipo_orden;
    }

    public Integer getNo_documento() {
        return no_documento;
    }

    public void setNo_documento(Integer no_documento) {
        this.no_documento = no_documento;
    }

    public Date getFecha_documento() {
        return fecha_documento;
    }

    public void setFecha_documento(Date fecha_documento) {
        this.fecha_documento = fecha_documento;
    }

    public String getEnviado() {
        return enviado;
    }

    public void setEnviado(String enviado) {
        this.enviado = enviado;
    }

    public String getObservacion() {
        return observacion;
    }

    public void setObservacion(String observacion) {
        this.observacion = observacion;
    }
    
}
