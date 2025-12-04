# Levantar todo
docker-compose up -d

# Ver el estado
docker-compose ps

# Ver logs
docker-compose logs -f postgres

# Detener
docker-compose stop

# Detener y eliminar contenedores (los datos persisten)
docker-compose down

# Detener, eliminar contenedores Y eliminar datos
docker-compose down -v



## activar entorno virtual
source venv/Scripts/activate

## desactivar entorno virtual
deactivate
