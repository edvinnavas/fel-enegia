# FETCH BASIC IMAGE.
FROM maven:3.8.5-jdk-11

# APPLICATION PLACED INTO /opt/app.
RUN mkdir -p /FELGT_ENERGIA/py
RUN chmod -R 777 /FELGT_ENERGIA/py
RUN mkdir -p /FELGT_ENERGIA/pd
RUN chmod -R 777 /FELGT_ENERGIA/pd
RUN mkdir -p /opt/app
RUN echo "America/Guatemala" > /etc/timezone
WORKDIR /opt/app

# SELECTIVELY ADD THE POM FILE AND INSTALL DEPENDENCIES.
COPY pom.xml /opt/app/
RUN mvn install

# REST OF THE PROJECTS.
COPY src /opt/app/src
RUN mvn package

# LOCAL APPLICATION PORT
EXPOSE 8080

# EXECUTE IT
CMD ["mvn", "exec:java"]
