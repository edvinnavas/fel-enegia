package Entidad;

import java.io.Serializable;
import java.util.List;

public class DTE_FEL implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private DTE_FEL_ENCABEZADO dte_fel_encabezado;
    private List<DTE_FEL_DETALLE> dte_fel_detalle;
    private DTE_FEL_ASOCIADOS dte_fel_asociados;
    private DTE_FEL_TOTALES dte_fel_totales;

    public DTE_FEL(DTE_FEL_ENCABEZADO dte_fel_encabezado, List<DTE_FEL_DETALLE> dte_fel_detalle, DTE_FEL_ASOCIADOS dte_fel_asociados, DTE_FEL_TOTALES dte_fel_totales) {
        this.dte_fel_encabezado = dte_fel_encabezado;
        this.dte_fel_detalle = dte_fel_detalle;
        this.dte_fel_asociados = dte_fel_asociados;
        this.dte_fel_totales = dte_fel_totales;
    }

    public DTE_FEL_ENCABEZADO getDte_fel_encabezado() {
        return dte_fel_encabezado;
    }

    public void setDte_fel_encabezado(DTE_FEL_ENCABEZADO dte_fel_encabezado) {
        this.dte_fel_encabezado = dte_fel_encabezado;
    }

    public List<DTE_FEL_DETALLE> getDte_fel_detalle() {
        return dte_fel_detalle;
    }

    public void setDte_fel_detalle(List<DTE_FEL_DETALLE> dte_fel_detalle) {
        this.dte_fel_detalle = dte_fel_detalle;
    }

    public DTE_FEL_ASOCIADOS getDte_fel_asociados() {
        return dte_fel_asociados;
    }

    public void setDte_fel_asociados(DTE_FEL_ASOCIADOS dte_fel_asociados) {
        this.dte_fel_asociados = dte_fel_asociados;
    }

    public DTE_FEL_TOTALES getDte_fel_totales() {
        return dte_fel_totales;
    }

    public void setDte_fel_totales(DTE_FEL_TOTALES dte_fel_totales) {
        this.dte_fel_totales = dte_fel_totales;
    }
    
}
