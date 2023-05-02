sudo apt-get install file -qq
wget -q https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
file spark-3.3.1-bin-hadoop3.tgz
tar xf spark-3.3.1-bin-hadoop3.tgz
wget https://github.com/xerial/sqlite-jdbc/releases/download/3.41.2.1/sqlite-jdbc-3.41.2.1.jar
mv sqlite-jdbc-3.41.2.1.jar spark-3.3.1-bin-hadoop3/jars/.
