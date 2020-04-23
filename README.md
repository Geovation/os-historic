docker stop historic_data
docker rm historic_data
#Run the container
docker run --name historic_data -p 7432:5432 \
        -v "$PWD/host_data":"/opt/input_data" \
        -v "$PWD/db_data":"/postgresql/data" \
        -d historic_data 