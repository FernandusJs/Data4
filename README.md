### Create your python environment
Follow instructions on Canvas to create your python virtual environment.

### Making sure to use the correct java version

### Installing the required packages
1. Go to terminal in PyCharm
2. Run the command: pip install -re requirements.txt (it icludes all the neccecairy packages for this cource)
3. Make sure all is packages are installed correctly (if you have errors, try to solve them and rund the command again)
4. if the IDE asks to install jupyter, do so.
5. Download Apache Spark 3.3.2 (Feb 13 release): https://www.apache.org/dyn/closer.lua/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
6. Unpack the downloaded file to a folder with your hadoop tools (ex. c:/hadoopTools). Unpacked directory later referred as SPARK DIRECTORY
_ATTENTION: In windows: unpack it to a (sub)folder of your main drive. Do not install it in a user folder. This causes many problems_
7. Perform these extra steps in Windows:
   8. Download thet Hadoop winutils folder https://canvas.kdg.be/files/3251662/download?download_frd=1
   9. Unpack to the same (ex. c:/hadoopTools) directory (you should have a Spark and Hadoop directory now)
10. Add the necessery environment variabeles to your system 
    11. HADOOP_HOME =[PATH TO HADOOP DIRECTORY] (ex. C:\hadoopTools\hadoop-3.3.1\)
    12. SPARK_HOME = [PATH TO SPARK DIRECTORY] (ex. C:\hadoopTools\spark-3.3.2-bin-hadoop3\)
    13. Add %HADOOP_HOME%\bin to PATH environment-variable
    14. Add %SPARK_HOME%\bin to PATH environment-variable

%HADOOP_HOME%\bin  removed from path
Sparkt bin folder added to path

SPARK_HOME C:\hadoopTools\spark-3.1.1-bin-hadoop3.2