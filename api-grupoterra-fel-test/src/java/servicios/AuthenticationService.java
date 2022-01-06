package servicios;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.StringTokenizer;
import javax.naming.InitialContext;
import javax.sql.DataSource;

public class AuthenticationService implements Serializable {

    private static final long serialVersionUID = 1L;

    public boolean authenticate(String authCredentials) {
        if (null == authCredentials) {
            return false;
        }
        
        final String encodedUserPassword = authCredentials.replaceFirst("Basic" +  " ", "");
        String usernameAndPassword = null;
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(encodedUserPassword);
            usernameAndPassword = new String(decodedBytes, "UTF-8");
        } catch (Exception ex) {
            System.out.println("CLASE: " + this.getClass().getName() + " METODO: authenticate ERROR: " + ex.toString());
        }
        
        final StringTokenizer tokenizer = new StringTokenizer(usernameAndPassword, ":");
        final String username = tokenizer.nextToken();
        final String password = tokenizer.nextToken();
        
        boolean authenticationStatus = false;
        Connection conn = null;
        try {
            InitialContext ctx = new InitialContext();
            DataSource ds = (DataSource) ctx.lookup("ITAPP_JNDI");
            conn = ds.getConnection();
            
            conn.setAutoCommit(false);
            
            String cadenasql = "select * from rest_credential r where r.usuario='" + username + "' and contrasena=SHA1('" + password + "')";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(cadenasql);
            while(rs.next()) {
                authenticationStatus = true;
            }
            rs.close();
            stmt.close();
        } catch(Exception ex) {
            System.out.println("CLASE: " + this.getClass().getName() + " METODO: authenticate ERROR: " + ex.toString());
        } finally {
            try {
                if(conn != null) {
                    conn.close();
                }
            } catch (Exception ex) {
                System.out.println("CLASE: " + this.getClass().getName() + " METODO: authenticate - finally ERROR: " + ex.toString());
            }
        }
        
        return authenticationStatus;
    }
    
}
