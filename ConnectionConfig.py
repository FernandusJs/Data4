from configparser import ConfigParser
#Configparser is a helper class to read properties from a configuration file
config = ConfigParser()
config.read('config.ini') #Define connection properties is the config file

cn = "default" #This is the default connection-name. Create a "default" profile in config.ini

#Returns a jdbc connection string based on the connection properties. Works only for sqlServer connections.
def create_jdbc():
    return f"jdbc:sqlserver://{config.get(cn, 'host')}:{config.get(cn, 'port')};database={config.get(cn, 'database')};encrypt=true;trustServerCertificate=true"

# Set the connectionName that has to be used (if you don't want to use the default profile
def set_connection(connectionName):
    global cn
    cn = connectionName

#Returns a specific property from the connection profile in the config.ini
def get_Property(propertyName):
  return config.get(cn, propertyName)

#List with all the needed jars. If your sparkJob needs extra jors, add them here.
jars = ["./jars/mssql-jdbc-10.2.1.jre8.jar"]


