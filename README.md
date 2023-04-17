### 1. Install Python
1. Install the latest version of python 3.9 from https://www.python.org/downloads/ (WINDOWS: https://www.python.org/downloads/release/python-3913/)
2. WINDOWS: make sure to check the box "Add Python 3.9 to PATH" during installation
3. After installation, open a terminal and run the command: python --version (should be 3.9.x)


### 2. Making sure to use the correct Java version
1. If not already installed on your system, install a Java version 8 up to 17 (NOT 18 or 20!) from https://www.oracle.com/java/technologies/javase/
2. Make sure JAVA_HOME is set to the correct version of Java (8-17) and that it is added to the PATH environment variable. (ex. JAVA_HOME: C:\Program Files\Java\jdk-11.0.8\)

### 3. Loading this project in PyCharm
1. Open PyCharm
2. Select "Get from VCS"
3. Use https://gitlab.com/kdg-ti/bigdata/sparkdelta.git as the URL and provide your credentials for GitLab
4. Do not yet create a virtual environment if asked in a popup.
5. In Settings >Project Settings > Python Interpreter, select Add interpreter... > Add local interpreter > Environment New > Base: 3.9 > OK
6. Make sure this new created environment is selected as the interpreter for this project

### 4. Installing the required packages
1. Go to terminal in PyCharm
2. Run the command: pip install -re requirements.txt (it includes all the necessary packages for this course)
3. Make sure all the packages are installed correctly (if you have errors, try to solve them and run the command again)
4. If the IDE asks to install jupyter, do so.

### 5. Installing Apache Spark and Hadoop
1. Download Apache Spark 3.3.2 (Feb 13 release): https://www.apache.org/dyn/closer.lua/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
2. Unpack the downloaded file to a folder with your hadoop tools (ex. c:/hadoopTools). Unpacked directory later referred as SPARK DIRECTORY
_ATTENTION: In windows: unpack it to a (sub)folder of your main drive. Do not install it in a user folder. This causes many problems_
3. Perform these extra steps in Windows:
   4. Download thet Hadoop winutils folder https://canvas.kdg.be/files/3251662/download?download_frd=1
   5. Unpack to the same (ex. c:/hadoopTools) directory (you should have a Spark and Hadoop directory now)
6. Add the necessery environment variables to your system  (https://www.how2shout.com/how-to/how-to-add-environment-variables-in-windows-11.html)
    7. HADOOP_HOME =[PATH TO HADOOP DIRECTORY] (ex. C:\hadoopTools\hadoop-3.3.1\)
    8. SPARK_HOME = [PATH TO SPARK DIRECTORY] (ex. C:\hadoopTools\spark-3.3.2-bin-hadoop3\)
    9. Add %HADOOP_HOME%\bin to PATH environment-variable
    10. Add %SPARK_HOME%\bin to PATH environment-variable

### 6. Performing the first run
1. Open the installCheck.py file in PyCharm
2. Run all cells one by one and check if everything is working correctly
3. If you have errors, try to solve them and run the cells again.
4. If you are stuck and cannot solve the errors, contact the teacher.