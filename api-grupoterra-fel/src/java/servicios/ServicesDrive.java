package servicios;

import controladores.Control_Fel;
import java.io.Serializable;
import javax.ejb.Stateless;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Stateless
@Path("fel")
public class ServicesDrive implements Serializable {

    private static final long serialVersionUID = 1L;
    
    @POST
    @Path("drive")
    public String drive(String cadenasql) {
        String resultado = "";
        
        try {
            Control_Fel control_fel = new Control_Fel();
            resultado = control_fel.drive(cadenasql);
        } catch(Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }
        
        return resultado;
    }
    
    @PUT
    @Path("jdetofel/{compania}/{fecha_inicial}/{fecha_final}")
    public String jde_to_fel(
            @PathParam("compania") String compania,
            @PathParam("fecha_inicial") Long fecha_inicial,
            @PathParam("fecha_final") Long fecha_final) {
        String resultado = "";
        
        try {
            Control_Fel control_fel = new Control_Fel();
            
            resultado = control_fel.Cargar_Doc_Fel(compania, fecha_inicial, fecha_final);
            String[] result = resultado.split("♣");
            if (result[0].equals("1")) {
                throw new Exception(result[1]);
            }
            
            resultado = control_fel.actualizar_descripcion_producto();
            result = resultado.split("♣");
            if (result[0].equals("1")) {
                throw new Exception(result[1]);
            }
            
            resultado = result[1];
            
        } catch(Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }
        
        return resultado;
    }
    
    @PUT
    @Path("desmarcar/{id_dte}")
    public String desmarcar(@PathParam("id_dte") Long id_dte) {
        String resultado = "";
        
        try {
            Control_Fel control_fel = new Control_Fel();
            resultado = control_fel.desmarcar_fel(id_dte);
        } catch(Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }
        
        return resultado;
    }
    
    @POST
    @Path("generar_archivo")
    public String generar_archivo(String JsonString) {
        String resultado = "";
        
        try {
            Control_Fel control_fel = new Control_Fel();
            resultado = control_fel.generar_archivo(JsonString);
        } catch(Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }
        
        return resultado;
    }
    
    @PUT
    @Path("modificar_ambiente/{id_ambiente}")
    public String modificar_ambiente(
            @PathParam("id_ambiente") Long id_ambiente,
            String valor) {
        
        String resultado = "";
        
        try {
            Control_Fel control_fel = new Control_Fel();
            resultado = control_fel.fel_modificar_ambiente(id_ambiente, Integer.parseInt(valor));
        } catch(Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }
        
        return resultado;
    }
    
    @POST
    @Path("autorizar")
    public String autorizar(String path) {
        String resultado = "";
        
        try {
            String[] param = path.split("♣");
            Control_Fel control_fel = new Control_Fel();
            resultado = control_fel.documento_autorizar(param[0], param[1], param[2]);
        } catch(Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }
        
        return resultado;
    }
    
}
