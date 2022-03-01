class functions:

    def __init__(self, connection: object, cursor: object):
        self.conn = connection
        self.cursor = cursor

    def startUpCheck(self):
        import os

        #check if the required folder exists
        dirs = ["data", "logs", "master_data"]
        logFiles = ["logs.log", "transactionLogs.csv"]
        for dir in dirs:
            if not os.path.exists(dir):
                os.makedirs(dir)
            else:
                pass
        for file in logFiles:
            dirPath = './logs/' + file
            if not os.path.exists(dirPath):
                open(dirPath, 'w').close()

    #Fetch surveyNumber and dataSource from DB
    def surveyNumber_dataSource(self):
        surveyNumberQuery = "select max(survey_id) from vestergaard_survey_master"
        self.cursor.execute(surveyNumberQuery)
        surveyNumber = self.cursor.fetchone()[0] + 1
        # print(surveyNumber, "surveyNumber")

        dataSourceQuery = "select * from vestergaard_datasource_master"
        self.cursor.execute(dataSourceQuery)
        dataSource = [i[1] for i in self.cursor.fetchall()]

        return surveyNumber, dataSource

    #Bulk upload to DB
    def bulkUploadCSV(self, file, table):
        data = open(file, 'r')
        query = f"COPY {table} FROM STDIN DELIMITER ',' CSV HEADER"
        self.cursor.copy_expert(query, data)
        #commit changes
        self.conn.commit()
        #close the file
        data.close()

    #writing logs to CSV
    def transactionLogs(self, log):
        import csv
        with open('./logs/transactionLogs.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow(log)

    def dateTime(self):
        import datetime
        dateTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date = dateTime.split(" ")[0]
        time = dateTime.split(" ")[1]
        return date, time

    def logging(self, level, message):
        import logging
        logging.basicConfig(filename='./logs/logs.log',
                            level=logging.INFO,
                            format='[%(levelname)s]  %(message)s')

        if level == "debug": logging.debug(f"{message}")
        elif level == "info": logging.info(f"{message}")
        elif level == "warning": logging.warning(f"{message}")
        elif level == "error": logging.error(f"{message}")
        elif level == "critical": logging.critical(f"{message}")
