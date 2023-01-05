package Entidad;

import java.io.Serializable;

public class FEL_DTE implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private Integer id_dte;
    private String kcoo_compania_jde;
    private Integer an8_cliente_jde;
    private Integer shan_sucursal_cliente_jde;
    private String nombre_cliente_jde;
    private Integer doco_no_orden_jde;
    private String dcto_tipo_orden_jde;
    private Integer doc_no_documento_jde;
    private Long fecha_documento;
    private Long fecha_hora_carga;
    private Long fecha_hora_envio;
    private String enviado;
    private String ambiente;
    private String observacion;

    public FEL_DTE(Integer id_dte, String kcoo_compania_jde, Integer an8_cliente_jde, Integer shan_sucursal_cliente_jde, String nombre_cliente_jde, Integer doco_no_orden_jde, String dcto_tipo_orden_jde, Integer doc_no_documento_jde, Long fecha_documento, Long fecha_hora_carga, Long fecha_hora_envio, String enviado, String ambiente, String observacion) {
        this.id_dte = id_dte;
        this.kcoo_compania_jde = kcoo_compania_jde;
        this.an8_cliente_jde = an8_cliente_jde;
        this.shan_sucursal_cliente_jde = shan_sucursal_cliente_jde;
        this.nombre_cliente_jde = nombre_cliente_jde;
        this.doco_no_orden_jde = doco_no_orden_jde;
        this.dcto_tipo_orden_jde = dcto_tipo_orden_jde;
        this.doc_no_documento_jde = doc_no_documento_jde;
        this.fecha_documento = fecha_documento;
        this.fecha_hora_carga = fecha_hora_carga;
        this.fecha_hora_envio = fecha_hora_envio;
        this.enviado = enviado;
        this.ambiente = ambiente;
        this.observacion = observacion;
    }

    public Integer getId_dte() {
        return id_dte;
    }

    public void setId_dte(Integer id_dte) {
        this.id_dte = id_dte;
    }

    public String getKcoo_compania_jde() {
        return kcoo_compania_jde;
    }

    public void setKcoo_compania_jde(String kcoo_compania_jde) {
        this.kcoo_compania_jde = kcoo_compania_jde;
    }

    public Integer getAn8_cliente_jde() {
        return an8_cliente_jde;
    }

    public void setAn8_cliente_jde(Integer an8_cliente_jde) {
        this.an8_cliente_jde = an8_cliente_jde;
    }

    public Integer getShan_sucursal_cliente_jde() {
        return shan_sucursal_cliente_jde;
    }

    public void setShan_sucursal_cliente_jde(Integer shan_sucursal_cliente_jde) {
        this.shan_sucursal_cliente_jde = shan_sucursal_cliente_jde;
    }

    public String getNombre_cliente_jde() {
        return nombre_cliente_jde;
    }

    public void setNombre_cliente_jde(String nombre_cliente_jde) {
        this.nombre_cliente_jde = nombre_cliente_jde;
    }

    public Integer getDoco_no_orden_jde() {
        return doco_no_orden_jde;
    }

    public void setDoco_no_orden_jde(Integer doco_no_orden_jde) {
        this.doco_no_orden_jde = doco_no_orden_jde;
    }

    public String getDcto_tipo_orden_jde() {
        return dcto_tipo_orden_jde;
    }

    public void setDcto_tipo_orden_jde(String dcto_tipo_orden_jde) {
        this.dcto_tipo_orden_jde = dcto_tipo_orden_jde;
    }

    public Integer getDoc_no_documento_jde() {
        return doc_no_documento_jde;
    }

    public void setDoc_no_documento_jde(Integer doc_no_documento_jde) {
        this.doc_no_documento_jde = doc_no_documento_jde;
    }

    public Long getFecha_documento() {
        return fecha_documento;
    }

    public void setFecha_documento(Long fecha_documento) {
        this.fecha_documento = fecha_documento;
    }

    public Long getFecha_hora_carga() {
        return fecha_hora_carga;
    }

    public void setFecha_hora_carga(Long fecha_hora_carga) {
        this.fecha_hora_carga = fecha_hora_carga;
    }

    public Long getFecha_hora_envio() {
        return fecha_hora_envio;
    }

    public void setFecha_hora_envio(Long fecha_hora_envio) {
        this.fecha_hora_envio = fecha_hora_envio;
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

    public String getAmbiente() {
        return ambiente;
    }

    public void setAmbiente(String ambiente) {
        this.ambiente = ambiente;
    }
    
}
