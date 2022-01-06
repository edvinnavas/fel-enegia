package entidades;

import java.io.Serializable;

public class DTE_FEL_TOTALES implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private Long id_dte;
    private Integer tipo_registro;
    private Double importe_bruto;
    private Double importe_descuento;
    private Double importe_exento;
    private Double importe_neto;
    private Double importe_iva;
    private Double importe_otros;
    private Double importe_total;
    private Double porcentaje_isr;
    private Double importe_isr;
    private Integer registros_detalle;
    private Integer documentos_asociados;

    public DTE_FEL_TOTALES(Long id_dte, Integer tipo_registro, Double importe_bruto, Double importe_descuento, Double importe_exento, Double importe_neto, Double importe_iva, Double importe_otros, Double importe_total, Double porcentaje_isr, Double importe_isr, Integer registros_detalle, Integer documentos_asociados) {
        this.id_dte = id_dte;
        this.tipo_registro = tipo_registro;
        this.importe_bruto = importe_bruto;
        this.importe_descuento = importe_descuento;
        this.importe_exento = importe_exento;
        this.importe_neto = importe_neto;
        this.importe_iva = importe_iva;
        this.importe_otros = importe_otros;
        this.importe_total = importe_total;
        this.porcentaje_isr = porcentaje_isr;
        this.importe_isr = importe_isr;
        this.registros_detalle = registros_detalle;
        this.documentos_asociados = documentos_asociados;
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

    public Double getImporte_bruto() {
        return importe_bruto;
    }

    public void setImporte_bruto(Double importe_bruto) {
        this.importe_bruto = importe_bruto;
    }

    public Double getImporte_descuento() {
        return importe_descuento;
    }

    public void setImporte_descuento(Double importe_descuento) {
        this.importe_descuento = importe_descuento;
    }

    public Double getImporte_exento() {
        return importe_exento;
    }

    public void setImporte_exento(Double importe_exento) {
        this.importe_exento = importe_exento;
    }

    public Double getImporte_neto() {
        return importe_neto;
    }

    public void setImporte_neto(Double importe_neto) {
        this.importe_neto = importe_neto;
    }

    public Double getImporte_iva() {
        return importe_iva;
    }

    public void setImporte_iva(Double importe_iva) {
        this.importe_iva = importe_iva;
    }

    public Double getImporte_otros() {
        return importe_otros;
    }

    public void setImporte_otros(Double importe_otros) {
        this.importe_otros = importe_otros;
    }

    public Double getImporte_total() {
        return importe_total;
    }

    public void setImporte_total(Double importe_total) {
        this.importe_total = importe_total;
    }

    public Double getPorcentaje_isr() {
        return porcentaje_isr;
    }

    public void setPorcentaje_isr(Double porcentaje_isr) {
        this.porcentaje_isr = porcentaje_isr;
    }

    public Double getImporte_isr() {
        return importe_isr;
    }

    public void setImporte_isr(Double importe_isr) {
        this.importe_isr = importe_isr;
    }

    public Integer getRegistros_detalle() {
        return registros_detalle;
    }

    public void setRegistros_detalle(Integer registros_detalle) {
        this.registros_detalle = registros_detalle;
    }

    public Integer getDocumentos_asociados() {
        return documentos_asociados;
    }

    public void setDocumentos_asociados(Integer documentos_asociados) {
        this.documentos_asociados = documentos_asociados;
    }
    
}
