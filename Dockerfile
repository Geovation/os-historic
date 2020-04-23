FROM postgis/postgis
ENV POSTGRES_PASSWORD=andreas
ENV PGDATA=/postgresql/data
RUN apt-get update
RUN apt-get -y install vim
COPY pg.conf /etc/postgresql/postgresql.conf
RUN mkdir /opt/input_data/
CMD ["-c", "config_file=/etc/postgresql/postgresql.conf"]