package com.bot.notificationservice.filter;

import com.fierhub.configures.ValidateRoute;
import com.fierhub.model.UserSession;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;

@Component
@Order(1)
public class RequestFilter implements Filter {
    @Autowired
    UserSession userSession;

    @Autowired
    private ValidateRoute validateRoute;
    private final static Logger LOGGER = LoggerFactory.getLogger(RequestFilter.class);

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws ServletException, IOException {
        try {
            HttpServletRequest request = ((HttpServletRequest) servletRequest);
            LOGGER.info("[NOTIFICATION SERVICE REQUEST]: " + request.getRequestURI());

            if (validateRoute.isSecured.test(request)) {
                filterChain.doFilter(servletRequest, servletResponse);
                return;
            } else {
                System.out.println("🔹 User Id: " + userSession.getUserId());
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unauthorized access. Error: " + ex.getMessage());
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }
}
