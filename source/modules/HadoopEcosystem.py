import subprocess

class HadoopEcosystem:

    def __init__(self) -> None:
        pass


    def start_spark(self):
        """
            Start all Spark service
        
            - Args:
                None

            Reture:
                None
        """
        command = "/home/thanhphat/BigData/spark-3.5.0-bin-hadoop3/sbin/start-all.sh"
        subprocess.run(command, shell=True, capture_output=True, text=True)


    def start_hadoop(self):
        """
            Start all Hadoop service
        
            - Args:
                None

            Reture:
                None
        """

        command = "start-all.sh"
        subprocess.run(command, shell=True, capture_output=True, text=True)
    
    def stop_hadoop(self):
        """
            Stop all Hadoop service
        
            - Args:
                None

            Reture:
                None
        """

        command = "stop-all.sh"
        subprocess.run(command, shell=True, capture_output=True, text=True)
    

    def start_superset(self):
        """
            Start all Superset service
        
            - Args:
                None

            Reture:
                None
        """

        command_1 = "source superset/bin/activate"
        subprocess.run(command_1, shell=True, capture_output=True, text=True)

        command_2 = "export SUPERSET_SECRET_KEY=""superset-real-time"" "
        subprocess.run(command_2, shell=True, capture_output=True, text=True)

        command_3 = "export FLASK_APP=""superset"""
        subprocess.run(command_3, shell=True, capture_output=True, text=True)

        command_4 = "superset run -p 9090 --with-threads --reload --debugger"
        subprocess.run(command_4, shell=True, capture_output=True, text=True)


    def start_hive(self):
        """
            Start all Hive server 2
        
            - Args:
                None

            - Connection: 
                hive://APP@localhost:10000/   

            Reture:
                None
        """
        

        command = "	./hive --service hiveserver2 --hiveconf hive.server2.thrift.port=10000 --hiveconf hive.root.logger=INFO,console --hiveconf hive.server2.enable.doAs=false"

        subprocess.run(command, shell=True, capture_output=True, text=True)

    def start_spark(self):
        pass


