package entidades;

import java.io.Serializable;

public class DTE_FEL_ENCABEZADO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private Long id_dte;
    private Integer tipo_registro;
    private Long fecha_documento;
    private String id_tipo_documento;
    private String nit_comprador;
    private Integer id_codigo_moneda;
    private Double tasa_cambio;
    private String orden_externo;
    private String id_tipo_venta;
    private Integer id_destino_venta;
    private String enviar_correo;
    private String nombre_comprador;
    private String direccion;
    private Long numero_acceso;
    private String serie_admin;
    private Long numero_admin;
    private Long ab_cliente;
    private String incoterm;
    private String codigo_exportador;

    public DTE_FEL_ENCABEZADO(Long id_dte, Integer tipo_registro, Long fecha_documento, String id_tipo_documento, String nit_comprador, Integer id_codigo_moneda, Double tasa_cambio, String orden_externo, String id_tipo_venta, Integer id_destino_venta, String enviar_correo, String nombre_comprador, String direccion, Long numero_acceso, String serie_admin, Long numero_admin, Long ab_cliente, String incoterm, String codigo_exportador) {
        this.id_dte = id_dte;
        this.tipo_registro = tipo_registro;
        this.fecha_documento = fecha_documento;
        this.id_tipo_documento = id_tipo_documento;
        this.nit_comprador = nit_comprador;
        this.id_codigo_moneda = id_codigo_moneda;
        this.tasa_cambio = tasa_cambio;
        this.orden_externo = orden_externo;
        this.id_tipo_venta = id_tipo_venta;
        this.id_destino_venta = id_destino_venta;
        this.enviar_correo = enviar_correo;
        this.nombre_comprador = nombre_comprador;
        this.direccion = direccion;
        this.numero_acceso = numero_acceso;
        this.serie_admin = serie_admin;
        this.numero_admin = numero_admin;
        this.ab_cliente = ab_cliente;
        this.incoterm = incoterm;
        this.codigo_exportador = codigo_exportador;
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

    public Long getFecha_documento() {
        return fecha_documento;
    }

    public void setFecha_documento(Long fecha_documento) {
        this.fecha_documento = fecha_documento;
    }

    public String getId_tipo_documento() {
        return id_tipo_documento;
    }

    public void setId_tipo_documento(String id_tipo_documento) {
        this.id_tipo_documento = id_tipo_documento;
    }

    public String getNit_comprador() {
        return nit_comprador;
    }

    public void setNit_comprador(String nit_comprador) {
        this.nit_comprador = nit_comprador;
    }

    public Integer getId_codigo_moneda() {
        return id_codigo_moneda;
    }

    public void setId_codigo_moneda(Integer id_codigo_moneda) {
        this.id_codigo_moneda = id_codigo_moneda;
    }

    public Double getTasa_cambio() {
        return tasa_cambio;
    }

    public void setTasa_cambio(Double tasa_cambio) {
        this.tasa_cambio = tasa_cambio;
    }

    public String getOrden_externo() {
        return orden_externo;
    }

    public void setOrden_externo(String orden_externo) {
        this.orden_externo = orden_externo;
    }

    public String getId_tipo_venta() {
        return id_tipo_venta;
    }

    public void setId_tipo_venta(String id_tipo_venta) {
        this.id_tipo_venta = id_tipo_venta;
    }

    public Integer getId_destino_venta() {
        return id_destino_venta;
    }

    public void setId_destino_venta(Integer id_destino_venta) {
        this.id_destino_venta = id_destino_venta;
    }

    public String getEnviar_correo() {
        return enviar_correo;
    }

    public void setEnviar_correo(String enviar_correo) {
        this.enviar_correo = enviar_correo;
    }

    public String getNombre_comprador() {
        return nombre_comprador;
    }

    public void setNombre_comprador(String nombre_comprador) {
        this.nombre_comprador = nombre_comprador;
    }

    public String getDireccion() {
        return direccion;
    }

    public void setDireccion(String direccion) {
        this.direccion = direccion;
    }

    public Long getNumero_acceso() {
        return numero_acceso;
    }

    public void setNumero_acceso(Long numero_acceso) {
        this.numero_acceso = numero_acceso;
    }

    public String getSerie_admin() {
        return serie_admin;
    }

    public void setSerie_admin(String serie_admin) {
        this.serie_admin = serie_admin;
    }

    public Long getNumero_admin() {
        return numero_admin;
    }

    public void setNumero_admin(Long numero_admin) {
        this.numero_admin = numero_admin;
    }

    public Long getAb_cliente() {
        return ab_cliente;
    }

    public void setAb_cliente(Long ab_cliente) {
        this.ab_cliente = ab_cliente;
    }

    public String getIncoterm() {
        return incoterm;
    }

    public void setIncoterm(String incoterm) {
        this.incoterm = incoterm;
    }

    public String getCodigo_exportador() {
        return codigo_exportador;
    }

    public void setCodigo_exportador(String codigo_exportador) {
        this.codigo_exportador = codigo_exportador;
    }
    
}
