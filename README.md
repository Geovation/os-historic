#Docker container for processing history data

##
```
docker container stop historic_data 
docker container rm historic_data
docker build -t historic_data .
docker run --name historic_data -p 7432:5432 \
        -v "$PWD/host_data":"/opt/input_data" \
        -v "$PWD/db_data":"/postgresql/data" \
        -d historic_data 
```