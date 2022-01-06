package servicios;

import java.io.Serializable;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("services")
public class ServiceConfig extends Application implements Serializable {

    private static final long serialVersionUID = 1L;

}
