package Entidad;

import java.io.Serializable;

public class DTE_FEL_DETALLE implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private Long id_dte;
    private Integer tipo_registro;
    private Long id_detalle;
    private Double cantidad;
    private Integer id_unidad_medida;
    private Double precio;
    private Double porcentaje_descuento;
    private Double importe_descuento;
    private Double importe_bruto;
    private Double importe_exento;
    private Double importe_neto;
    private Double importe_iva;
    private Double importe_otros;
    private Double importe_total;
    private String producto;
    private String descipcion;
    private String id_tipo_venta;

    public DTE_FEL_DETALLE(Long id_dte, Integer tipo_registro, Long id_detalle, Double cantidad, Integer id_unidad_medida, Double precio, Double porcentaje_descuento, Double importe_descuento, Double importe_bruto, Double importe_exento, Double importe_neto, Double importe_iva, Double importe_otros, Double importe_total, String producto, String descipcion, String id_tipo_venta) {
        this.id_dte = id_dte;
        this.tipo_registro = tipo_registro;
        this.id_detalle = id_detalle;
        this.cantidad = cantidad;
        this.id_unidad_medida = id_unidad_medida;
        this.precio = precio;
        this.porcentaje_descuento = porcentaje_descuento;
        this.importe_descuento = importe_descuento;
        this.importe_bruto = importe_bruto;
        this.importe_exento = importe_exento;
        this.importe_neto = importe_neto;
        this.importe_iva = importe_iva;
        this.importe_otros = importe_otros;
        this.importe_total = importe_total;
        this.producto = producto;
        this.descipcion = descipcion;
        this.id_tipo_venta = id_tipo_venta;
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

    public Long getId_detalle() {
        return id_detalle;
    }

    public void setId_detalle(Long id_detalle) {
        this.id_detalle = id_detalle;
    }

    public Double getCantidad() {
        return cantidad;
    }

    public void setCantidad(Double cantidad) {
        this.cantidad = cantidad;
    }

    public Integer getId_unidad_medida() {
        return id_unidad_medida;
    }

    public void setId_unidad_medida(Integer id_unidad_medida) {
        this.id_unidad_medida = id_unidad_medida;
    }

    public Double getPrecio() {
        return precio;
    }

    public void setPrecio(Double precio) {
        this.precio = precio;
    }

    public Double getPorcentaje_descuento() {
        return porcentaje_descuento;
    }

    public void setPorcentaje_descuento(Double porcentaje_descuento) {
        this.porcentaje_descuento = porcentaje_descuento;
    }

    public Double getImporte_descuento() {
        return importe_descuento;
    }

    public void setImporte_descuento(Double importe_descuento) {
        this.importe_descuento = importe_descuento;
    }

    public Double getImporte_bruto() {
        return importe_bruto;
    }

    public void setImporte_bruto(Double importe_bruto) {
        this.importe_bruto = importe_bruto;
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

    public String getProducto() {
        return producto;
    }

    public void setProducto(String producto) {
        this.producto = producto;
    }

    public String getDescipcion() {
        return descipcion;
    }

    public void setDescipcion(String descipcion) {
        this.descipcion = descipcion;
    }

    public String getId_tipo_venta() {
        return id_tipo_venta;
    }

    public void setId_tipo_venta(String id_tipo_venta) {
        this.id_tipo_venta = id_tipo_venta;
    }
    
}
