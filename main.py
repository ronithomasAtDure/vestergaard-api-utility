"""
author: roni.thomas@duretechnologies.com
date: 09-02-2022
"""

from flask import Blueprint, render_template, request, send_from_directory, redirect, url_for, Response
from flask_login import login_required, current_user
from __init__ import create_app, db
import os
import time
from hurry.filesize import size
import vestergaard_api_extraction as vae
import pandas as pd
import functions as fns

main = Blueprint('main', __name__)

#creating DB connection
conn = vae.connection()
cursor = conn.cursor()

#initiating required functions
fns = fns.functions(conn, cursor)

#creating required folders if not exists
fns.startUpCheck()


#index page/login page
@main.route("/")
def login():
    return render_template("login.html")


#logout page
@main.route("/logout/")
def logout():
    fns.logging("info",
                f"{current_user.username} logged out, {fns.dateTime()}")
    return render_template("logout.html")


#logging page
@main.route("/logs/")
def logs():
    logs = []
    with open("./logs/logs.log", "r") as file:
        logging = file.readlines()
        for log in logging[::-1]:
            logs.append(log)

    return render_template("logs.html", logs=logs)


#dashboard page
@main.route("/dashboard/")
@login_required
def dashboard():
    fns.logging(
        "info",
        f"{current_user.username} accessed dashboard, on {fns.dateTime()[0]} at {fns.dateTime()[1]}"
    )
    return render_template("dashboard.html")


#fetches the data from the DB
@main.route("/fetch-data/", methods=['POST', 'GET'])
@login_required
def fetchData():
    existingMasterDataQuery = "select * from vestergaard_survey_master"
    existingMasterData = pd.read_sql(existingMasterDataQuery, conn)
    print(existingMasterData)
    surveyIDs = existingMasterData.survey_id.to_list()
    surveyNames = existingMasterData.survey_name.to_list()
    surveyStartDates = existingMasterData.survey_start_date.to_list()
    surveyEndDates = existingMasterData.survey_end_date.to_list()

    #capturing the form data
    if request.method == "POST":
        apiurl = request.form['apiurl']
        startDate = request.form['startDate']
        startDate = startDate + " 00:00:00"
        endDate = request.form['endDate']
        endDate = endDate + " 23:59:59"
        surveyName = request.form['surveyName']
        dataSource = request.form['dataSource']
        fileType = request.form['fileType']
        # print(apiurl, startDate, endDate, surveyName)

        surveyNumber, dataSourceList = fns.surveyNumber_dataSource()
        surveyNumber = request.form['surveyNumber']

        #calling the extraction function
        vae.extraction(apiurl, startDate, endDate, fileType, surveyNumber,
                       dataSource)

        fns.logging(
            "info",
            f"{current_user.username} has extracted data from {apiurl}")

        #make an entry into vestergaard_survey_master
        cursor.execute(
            """INSERT INTO vestergaard_survey_master (survey_id, survey_name, survey_country, survey_start_date, survey_end_date)
                        VALUES (%s, %s, %s, %s, %s)""",
            (surveyNumber, surveyName, dataSource, startDate, endDate))
        conn.commit()
        fns.logging(
            "info",
            f"{current_user.username} has added survey {surveyNumber} to DB")

        return render_template("fetch-data.html",
                               surveyIDs=surveyIDs,
                               surveyNames=surveyNames,
                               surveyStartDates=surveyStartDates,
                               surveyEndDates=surveyEndDates,
                               surveyNumber=surveyNumber + 1,
                               dataSource=dataSourceList)

    else:
        surveyNumber, dataSource = fns.surveyNumber_dataSource()
        return render_template("fetch-data.html",
                               surveyIDs=surveyIDs,
                               surveyNames=surveyNames,
                               surveyStartDates=surveyStartDates,
                               surveyEndDates=surveyEndDates,
                               surveyNumber=surveyNumber,
                               dataSource=dataSource)


#uploads the downloaded data to the DB
@main.route("/db-upload/", methods=['POST', 'GET'])
@login_required
def dbupload():
    #gather file name from the uploads folder
    fileData = []
    for files in os.listdir("./data"):
        fileData.append(files)

    #capturing the form data
    if request.method == "POST":
        uploadFile = request.form['uploadFile']
        uploadType = request.form['uploadType']
        print(type(uploadType))
        # print(fileName)
        surveyNumber = uploadFile.split('_')[0]
        checkSurveyQuery = f"select * from vestergaard_survey_master vsm where survey_id={int(surveyNumber)}"
        cursor.execute(checkSurveyQuery)
        if cursor.rowcount == 0 or uploadType == '1':
            #1 = New Data
            #2 = Replace data with existing survey ID
            if uploadFile.endswith(".csv"):
                #load the csv file into the staging table
                file = "./data/" + str(uploadFile)
                fns.bulkUploadCSV(file, "vestergaard_api_stg_data")

            elif uploadFile.endswith(".json"):
                #read the json file and convert it to a temp csv
                data = pd.read_json("./data/" + uploadFile)
                data = data.to_csv("./data/" + uploadFile + ".csv",
                                   index=False)

                #load the csv file into the staging table
                file = "./data/" + uploadFile + ".csv"
                fns.bulkUploadCSV(file, "vestergaard_api_stg_data")

                #remove temp csv file
                os.remove("./data/" + uploadFile + ".csv")

            fns.logging(
                "info",
                f"{current_user.username} has uploaded {uploadFile} to DB")
            fileData.remove(uploadFile)
            transactionFileList.append(uploadFile)

        elif uploadType == '2':
            deleteQuery = f"delete from vestergaard_api_stg_data where survey_id={int(surveyNumber)}"
            transactionQuery1 = f"delete from vestaguard_etl_staging where survey_type={int(surveyNumber)}"
            transactionQuery2 = f"delete from vestagaard_data_survey where survey_type={int(surveyNumber)}"
            cursor.execute(deleteQuery)
            cursor.execute(transactionQuery1)
            cursor.execute(transactionQuery2)
            conn.commit()

            if uploadFile.endswith(".csv"):
                #load the csv file into the staging table
                file = "./data/" + str(uploadFile)
                fns.bulkUploadCSV(file, "vestergaard_api_stg_data")

            elif uploadFile.endswith(".json"):
                #read the json file and convert it to a temp csv
                data = pd.read_json("./data/" + uploadFile)
                data = data.to_csv("./data/" + uploadFile + ".csv",
                                   index=False)

                #load the csv file into the staging table
                file = "./data/" + uploadFile + ".csv"
                fns.bulkUploadCSV(file, "vestergaard_api_stg_data")

                #remove temp csv file
                os.remove("./data/" + uploadFile + ".csv")

            fns.logging(
                "info",
                f"{current_user.username} has uploaded {uploadFile} to DB")
            fileData.remove(uploadFile)
            transactionFileList.append(uploadFile)

        return render_template("db-upload.html", fileData=fileData)
    else:
        return render_template("db-upload.html", fileData=fileData)


#view files in download(data) folder
@main.route("/data-directory")
@login_required
def dataDirectory():
    #gather file name from the download folder
    fileData = []
    for files in os.listdir("./data"):
        fileName, fileType = os.path.splitext(files)
        fileDate = time.ctime(os.path.getctime("./data/" + files))
        fileSize = size(os.path.getsize("./data/" + files))
        fileData.append([fileName, fileType, fileDate, fileSize])

    return render_template("data-directory.html", fileData=fileData)


#downloading file
@main.route('/download/<path:filename>', methods=['GET', 'POST'])
@login_required
def download(filename):
    fns.logging("info", f"{current_user.username} has downloaded {filename}")
    return send_from_directory("./data", filename)


#deleting file
@main.route('/delete/<filename>', methods=['GET', 'POST'])
@login_required
def delete(filename):
    os.remove("./data/" + filename)
    fns.logging("critical", f"{current_user.username} has deleted {filename}")
    return redirect(url_for('main.dataDirectory'))


@main.route('/master-data', methods=['GET', 'POST'])
@login_required
def masterData():
    print("Status Code", Response.status)
    #gather available schema from query
    query = "select * from vestergaard_master_data"
    #storing result in a dataframe
    df = pd.read_sql(query, conn)

    #converting the dataframe to a list
    tableDesc = df.table_desc.values.tolist()
    fetchTable = df.fetch_table_name.values.tolist()
    uploadTable = df.upload_table_name.values.tolist()
    # print(tableDesc, fetchTable)

    if request.method == "POST":
        try:
            schemaDownload = request.form['schemaDownload']

            #download the schema
            query = f"select * from {schemaDownload} limit 0"
            cursor.execute(query)
            colnames = [colname[0] for colname in cursor.description]
            df = pd.DataFrame(columns=colnames)
            fns.logging(
                "info",
                f"{current_user.username} has downloaded {schemaDownload} schema"
            )
            return Response(df.to_csv(index=False), mimetype='text/csv')

        except:
            schemaUpload = request.form['schemaUpload']
            CSVfile = request.files['CSVfile']
            CSVfile.save("./master_data/" + CSVfile.filename)
            # print(CSVfile.filename)
            file = "./master_data/" + CSVfile.filename
            fns.bulkUploadCSV(file, schemaUpload)
            fns.logging(
                "info",
                f"{current_user.username} has uploaded {schemaUpload} schema")

            return render_template("master-data.html",
                                   tableDesc=tableDesc,
                                   fetchTable=fetchTable,
                                   uploadTable=uploadTable)
    else:
        return render_template("master-data.html",
                               tableDesc=tableDesc,
                               fetchTable=fetchTable,
                               uploadTable=uploadTable)


transactionFileList = []
transactionSessionLogs = []


@main.route('/transaction-data', methods=['GET', 'POST'])
@login_required
def transactionData():
    if request.method == "POST":
        try:
            fileName = request.form['fileName']
            query = f"select * from vestergaard_data_insert('{fileName}');"
            cursor.execute(query)
            status = cursor.fetchone()
            print("Query status: ", status[0])
            transactionFileList.remove(fileName)
            date, time = fns.dateTime()
            logs = [status[0], fileName, date, time]
            fns.transactionLogs(logs)
            transactionSessionLogs.append(logs)
            print(transactionSessionLogs)
            fns.logging(
                "info",
                f"{current_user.username} has transacted data {fileName}")
            return render_template(
                "transaction-data.html",
                transactionFileList=transactionFileList,
                transactionSessionLogs=transactionSessionLogs[::-1])
        except:
            logs = ["FAILED", fileName, date, time]
            fns.logging(
                "warning",
                f"{current_user.username} failed transacted data {fileName}")
            return render_template(
                "transaction-data.html",
                transactionFileList=transactionFileList,
                transactionSessionLogs=transactionSessionLogs[::-1])
    else:
        print(transactionFileList)
        return render_template(
            "transaction-data.html",
            transactionFileList=transactionFileList,
            transactionSessionLogs=transactionSessionLogs[::-1])


@main.route('/save-data', methods=['GET', 'POST'])
def saveData():
    uploadCSVfile = request.files['uploadCSVfile']
    uploadCSVfile.save("./data/" + uploadCSVfile.filename)
    # print(CSVfile.filename)
    fns.logging(
        "info",
        f"{current_user.username} has added {uploadCSVfile.filename} to folder"
    )


# we initialize our flask app using the __init__.py function
app = create_app()

#jinja zip for multiple iteration in html table
app.jinja_env.filters['zip'] = zip

if __name__ == '__main__':
    db.create_all(app=create_app())  # create the SQLite database
    app.run(port=8080, debug=True)  # run the flask app on debug mode
