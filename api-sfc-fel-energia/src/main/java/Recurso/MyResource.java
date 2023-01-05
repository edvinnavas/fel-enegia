package Recurso;

import Control.Ctrl_Base_Datos;
import Control.Ctrl_Fel;
import java.io.Serializable;
import java.sql.Connection;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("energia-felgt")
public class MyResource implements Serializable {

    private static final long serialVersionUID = 1L;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt() {
        return "Got it!";
    }

    @POST
    @Path("drive/{ambiente}")
    public String drive(
            @PathParam("ambiente") String ambiente,
            String cadenasql) {

        Connection conn;
        String resultado;

        try {
            Ctrl_Base_Datos ctrl_base_datos = new Ctrl_Base_Datos();
            conn = ctrl_base_datos.obtener_conexion(ambiente);

            Ctrl_Fel control_fel = new Ctrl_Fel();
            resultado = control_fel.drive(conn, cadenasql);
        } catch (Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }

        return resultado;
    }

    @PUT
    @Path("jdetofel/{ambiente}/{compania}/{fecha_inicial}/{fecha_final}")
    public String jde_to_fel(
            @PathParam("ambiente") String ambiente,
            @PathParam("compania") String compania,
            @PathParam("fecha_inicial") Long fecha_inicial,
            @PathParam("fecha_final") Long fecha_final) {
        
        Connection conn;
        String resultado;

        try {
            Ctrl_Base_Datos ctrl_base_datos = new Ctrl_Base_Datos();
            conn = ctrl_base_datos.obtener_conexion(ambiente);
            
            Ctrl_Fel control_fel = new Ctrl_Fel();
            resultado = control_fel.Cargar_Doc_Fel(conn, compania, fecha_inicial, fecha_final);
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

        } catch (Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }

        return resultado;
    }

    @PUT
    @Path("desmarcar/{ambiente}/{id_dte}")
    public String desmarcar(
            @PathParam("ambiente") String ambiente,
            @PathParam("id_dte") Long id_dte) {
        
        Connection conn;
        String resultado;

        try {
            Ctrl_Base_Datos ctrl_base_datos = new Ctrl_Base_Datos();
            conn = ctrl_base_datos.obtener_conexion(ambiente);
            
            Ctrl_Fel control_fel = new Ctrl_Fel();
            resultado = control_fel.desmarcar_fel(conn, id_dte);
        } catch (Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }

        return resultado;
    }

    @POST
    @Path("generar_archivo/{ambiente}")
    public String generar_archivo(
            @PathParam("ambiente") String ambiente,
            String JsonString) {
        
        Connection conn;
        String resultado;

        try {
            Ctrl_Base_Datos ctrl_base_datos = new Ctrl_Base_Datos();
            conn = ctrl_base_datos.obtener_conexion(ambiente);
            
            Ctrl_Fel control_fel = new Ctrl_Fel();
            resultado = control_fel.generar_archivo(conn, JsonString);
        } catch (Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }

        return resultado;
    }

    @PUT
    @Path("modificar_ambiente/{ambiente}/{id_ambiente}")
    public String modificar_ambiente(
            @PathParam("ambiente") String ambiente,
            @PathParam("id_ambiente") Long id_ambiente,
            String valor) {

        Connection conn;
        String resultado;

        try {
            Ctrl_Base_Datos ctrl_base_datos = new Ctrl_Base_Datos();
            conn = ctrl_base_datos.obtener_conexion(ambiente);
            
            Ctrl_Fel control_fel = new Ctrl_Fel();
            resultado = control_fel.fel_modificar_ambiente(conn, id_ambiente, Integer.valueOf(valor));
        } catch (Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }

        return resultado;
    }

    @POST
    @Path("autorizar/{ambiente}")
    public String autorizar(
            @PathParam("ambiente") String ambiente,
            String path) {
        
        Connection conn;
        String resultado;

        try {
            Ctrl_Base_Datos ctrl_base_datos = new Ctrl_Base_Datos();
            conn = ctrl_base_datos.obtener_conexion(ambiente);
            
            String[] param = path.split("♣");
            Ctrl_Fel control_fel = new Ctrl_Fel();
            resultado = control_fel.documento_autorizar(conn, param[0], param[1], param[2]);
        } catch (Exception ex) {
            resultado = "ERROR: " + ex.toString();
        }

        return resultado;
    }

}
