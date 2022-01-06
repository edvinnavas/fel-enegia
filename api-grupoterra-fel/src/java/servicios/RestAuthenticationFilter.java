package servicios;

import java.io.IOException;
import java.io.Serializable;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class RestAuthenticationFilter implements javax.servlet.Filter, Serializable {

    public static final String AUTHENTICATION_HEADER = "Authorization";
    private static final long serialVersionUID = 1L;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filter) throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest httpServletRequest = (HttpServletRequest) request;
            String authCredentials = httpServletRequest.getHeader(AUTHENTICATION_HEADER);
            AuthenticationService authenticationService = new AuthenticationService();
            boolean authenticationStatus = authenticationService.authenticate(authCredentials);
            if (authenticationStatus) {
                filter.doFilter(request, response);
            } else {
                if (response instanceof HttpServletResponse) {
                    HttpServletResponse httpServletResponse = (HttpServletResponse) response;
                    httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                }
            }
        }
    }

    @Override
    public void destroy() {
        
    }

    @Override
    public void init(FilterConfig arg0) throws ServletException {
        
    }
    
}
