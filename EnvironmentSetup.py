import os

#Change to the root path of you installation directories. (ex. c:\bigdatatools\spark-3.4.0-bin-hadoop3)
spark_home = "C:\DevApps\spark-3.4.0-bin-hadoop3"
hadoop_home = "C:\DevApps\winutils\winutils-master\hadoop-3.4.0-win10-x64"
java_home = "C:\Program Files\Java\jdk-11.0.8"


# Do not change!
def setupEnvironment():

    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["SPARK_HOME"] = spark_home
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PYSPARK_HADOOP_VERSION"] ="3"
    os.environ["JAVA_HOME"] = java_home + os.sep
    pathlist = [spark_home + os.sep + "bin", hadoop_home + os.sep +  "bin", java_home + os.sep + "bin"]
    os.environ["PATH"] += os.pathsep + os.pathsep.join(pathlist)

def listEnvironment():
    import os
    for key, value in os.environ.items():
        print(f'{key}: {value}')