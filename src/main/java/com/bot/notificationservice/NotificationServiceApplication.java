package com.bot.notificationservice;

import com.fierhub.configures.AutoConfigureServices;
import com.fierhub.configures.ValidateRoute;
import com.fierhub.model.FierhubConfig;
import com.fierhub.model.UserSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.web.context.WebApplicationContext;

import com.fierhub.service.FierhubService;
import com.fierhub.database.service.HttpRequestService;
import org.springframework.context.annotation.Import;

@SpringBootApplication(exclude = {
        AutoConfigureServices.class,
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
@Import({FierhubService.class, HttpRequestService.class})
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }

    @Bean
    public FierhubConfig fierhubConfig() {
        return FierhubConfig.getInstance();
    }

    @Bean
    public ValidateRoute validateRoute() {
        return new ValidateRoute();
    }

    @Bean
    @Scope(value = WebApplicationContext.SCOPE_REQUEST, proxyMode = ScopedProxyMode.TARGET_CLASS)
    public UserSession userSession() {
        return new UserSession();
    }
}
