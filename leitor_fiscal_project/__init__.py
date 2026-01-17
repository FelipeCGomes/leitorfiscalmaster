import pymysql

# Truque: Falsifica a versão do driver para o Django 5 aceitar
# Dizemos que somos a versão 2.2.2 do mysqlclient
pymysql.version_info = (2, 2, 2, "final", 0)

# Instala o driver como se fosse o padrão
pymysql.install_as_MySQLdb()