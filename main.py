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

def readTableConfig():
    global tableConfig
    tableConfig = pd.read_json('tableConfig.json', orient='records')


#index page/login page
@main.route("/")
def login():
    projectNamesQuery = "select * from projectNames"
    projectNames = pd.read_sql(projectNamesQuery, conn)
    return render_template("login.html",
                           projectNames=projectNames.project_name.tolist())


#logout page
@main.route("/logout/")
def logout():
    fns.logging("info", f"user logged out, {fns.dateTime()}")
    return render_template("logout.html")


#logging page
@main.route("/logs/")
def logs():
    logs = []
    with open("./logs/logs.log", "r") as file:
        logging = file.readlines()
        for log in logging[::-1]:
            logs.append(log)

    return render_template(
        "logs.html",
        logs=logs,
        projectName=tableConfig.project_name[0],
    )


#dashboard page
@main.route("/dashboard/")
@login_required
def dashboard():
    readTableConfig()
    #creating required folders if not exists
    fns.startUpCheck()
    fns.logging(
        "info",
        f"{current_user.username} accessed dashboard, on {fns.dateTime()[0]} at {fns.dateTime()[1]}"
    )
    return render_template("dashboard.html",
                           projectName=tableConfig.project_name[0])


#fetches the data from the DB
@main.route("/fetch-data/", methods=['POST', 'GET'])
@login_required
def fetchData():
    try:
        # print(tableConfig.project_name_survey_master[0])
        existingMasterDataQuery = f"select * from {tableConfig.project_name_survey_master[0]}"
        print(existingMasterDataQuery)
        existingMasterData = pd.read_sql(existingMasterDataQuery, conn)
        # print(existingMasterData)
        surveyIDs = existingMasterData.survey_id.to_list()
        print("surveyIDs", surveyIDs)
        surveyNames = existingMasterData.survey_name.to_list()
        surveyStartDates = existingMasterData.survey_start_date.to_list()
        surveyEndDates = existingMasterData.survey_end_date.to_list()
    except Exception as e:
        return render_template("404.html", error=e)

    #capturing the form data
    if request.method == "POST":
        try:
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

            if surveyNumber not in surveyIDs:
                print("Master data does not exists")
                #make an entry into vestergaard_survey_master
                cursor.execute(
                    f"""INSERT INTO {tableConfig.project_name_survey_master[0]} (survey_id, survey_name, survey_country, survey_start_date, survey_end_date)
                                VALUES (%s, %s, %s, %s, %s)""",
                    (surveyNumber, surveyName, dataSource, startDate, endDate))
                conn.commit()
                fns.logging(
                    "info",
                    f"{current_user.username} has added survey {surveyNumber} to DB"
                )
            else:
                #update the existing entry
                cursor.execute(
                    f"""update {tableConfig.project_name_survey_master[0]} set survey_name='{surveyName}', survey_country='{dataSource}', survey_start_date='{startDate}', survey_end_date='{endDate}'
                                where survey_id = '{surveyNumber}'""")
                conn.commit()
                fns.logging(
                    "info",
                    f"{current_user.username} has added survey {surveyNumber} to DB"
                )

            return redirect(url_for('main.dbupload'))

        except Exception as e:
            return render_template("404.html", error=e)

    else:
        surveyNumber, dataSource = fns.surveyNumber_dataSource()
        return render_template("fetch-data.html",
                               projectName=tableConfig.project_name[0],
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
    existingMasterDataQuery = f"select * from {tableConfig.project_name_survey_master[0]}"
    print(existingMasterDataQuery)
    existingMasterData = pd.read_sql(existingMasterDataQuery, conn)
    # print(existingMasterData)
    surveyIDs = existingMasterData.survey_id.to_list()

    #gather file name from the uploads folder
    fileData = []
    for files in os.listdir(f"./{tableConfig.project_name[0]}"):
        fileData.append(files)

    #capturing the form data
    if request.method == "POST":
        try:
            uploadFile = request.form['uploadFile']
            uploadType = request.form['uploadType']
            print(type(uploadType))
            # print(fileName)
            surveyNumber = uploadFile.split('_')[0]
            checkSurveyQuery = f"select * from {tableConfig.project_name_survey_master[0]} where survey_id={int(surveyNumber)}"
            cursor.execute(checkSurveyQuery)
            if cursor.rowcount == 0 or uploadType == '1':
                #1 = New Data
                #2 = Replace data with existing survey ID
                if uploadFile.endswith(".csv"):
                    #load the csv file into the staging table
                    file = f"./{tableConfig.project_name[0]}/" + str(uploadFile)
                    fns.bulkUploadCSV(file,
                                      tableConfig.project_name_api_stg_data[0])

                elif uploadFile.endswith(".json"):
                    #read the json file and convert it to a temp csv
                    data = pd.read_json(f"./{tableConfig.project_name[0]}/" + uploadFile)
                    data = data.to_csv(f"./{tableConfig.project_name[0]}/" + uploadFile + ".csv",
                                       index=False)

                    #load the csv file into the staging table
                    file = f"./{tableConfig.project_name[0]}/" + uploadFile + ".csv"
                    fns.bulkUploadCSV(file,
                                      tableConfig.project_name_api_stg_data[0])

                    #remove temp csv file
                    os.remove(f"./{tableConfig.project_name[0]}/" + uploadFile + ".csv")

                fns.logging(
                    "info",
                    f"{current_user.username} has uploaded {uploadFile} to DB")
                fileData.remove(uploadFile)
                transactionFileList.append(uploadFile)

            elif uploadType == '2':
                deleteQuery = f"delete from {tableConfig.project_name_api_stg_data[0]} where survey_id='{int(surveyNumber)}'"
                transactionQuery1 = f"delete from {tableConfig.project_name_etl_staging[0]} where survey_type='{int(surveyNumber)}'"
                transactionQuery2 = f"delete from {tableConfig.project_name_data_survey[0]} where survey_type='{int(surveyNumber)}'"
                cursor.execute(deleteQuery)
                cursor.execute(transactionQuery1)
                cursor.execute(transactionQuery2)
                conn.commit()

                if uploadFile.endswith(".csv"):
                    #load the csv file into the staging table
                    file = f"./{tableConfig.project_name[0]}/" + str(uploadFile)
                    fns.bulkUploadCSV(file,
                                      tableConfig.project_name_api_stg_data[0])

                elif uploadFile.endswith(".json"):
                    #read the json file and convert it to a temp csv
                    data = pd.read_json(f"./{tableConfig.project_name[0]}/" + uploadFile)
                    data = data.to_csv(f"./{tableConfig.project_name[0]}/" + uploadFile + ".csv",
                                       index=False)

                    #load the csv file into the staging table
                    file = f"./{tableConfig.project_name[0]}/" + uploadFile + ".csv"
                    fns.bulkUploadCSV(file,
                                      tableConfig.project_name_api_stg_data[0])

                    #remove temp csv file
                    os.remove(f"./{tableConfig.project_name[0]}/" + uploadFile + ".csv")

                fns.logging(
                    "info",
                    f"{current_user.username} has uploaded {uploadFile} to DB")
                fileData.remove(uploadFile)
                transactionFileList.append(uploadFile)

            return redirect(url_for('main.transactionData'))

        except Exception as e:
            return render_template("404.html", error=e)

    else:
        return render_template(
            "db-upload.html",
            projectName=tableConfig.project_name[0],
            fileData=fileData,
            uploadTableName=tableConfig.project_name_api_stg_data[0])


#view files in download(data) folder
@main.route("/data-directory")
@login_required
def dataDirectory():
    #gather file name from the download folder
    fileData = []
    for files in os.listdir(f"./{tableConfig.project_name[0]}"):
        fileName, fileType = os.path.splitext(files)
        fileDate = time.ctime(os.path.getctime(f"./{tableConfig.project_name[0]}/" + files))
        fileSize = size(os.path.getsize(f"./{tableConfig.project_name[0]}/" + files))
        fileData.append([fileName, fileType, fileDate, fileSize])

    return render_template("data-directory.html",
                           fileData=fileData,
                           projectName=tableConfig.project_name[0])


#downloading file
@main.route('/download/<path:filename>', methods=['GET', 'POST'])
@login_required
def download(filename):
    fns.logging("info", f"{current_user.username} has downloaded {filename}")
    return send_from_directory(f"./{tableConfig.project_name[0]}", filename)


#deleting file
@main.route('/delete/<filename>', methods=['GET', 'POST'])
@login_required
def delete(filename):
    os.remove(f"./{tableConfig.project_name[0]}/" + filename)
    fns.logging("critical", f"{current_user.username} has deleted {filename}")
    return redirect(url_for('main.dataDirectory'))


@main.route('/master-data', methods=['GET', 'POST'])
@login_required
def masterData():
    try:
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
    except Exception as e:
        return render_template("404.html", error=e)

    if request.method == "POST":
        try:
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
                    f"{current_user.username} has uploaded {schemaUpload} schema"
                )

                return render_template("master-data.html",
                                       projectName=tableConfig.project_name[0],
                                       tableDesc=tableDesc,
                                       fetchTable=fetchTable,
                                       uploadTable=uploadTable)

        except Exception as e:
            return render_template("404.html", error=e)
    else:
        return render_template("master-data.html",
                               projectName=tableConfig.project_name[0],
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
            try:
                fileName = request.form['fileName']
                query = f"select * from vestergaard_data_insert('{fileName}');"
                print(query)
                cursor.execute(query)
                conn.commit()
                status = cursor.fetchone()
                print("Query status: ", status[0])
                # transactionFileList.remove(fileName)
                date, time = fns.dateTime()
                logs = [status[0], fileName, date, time]
                fns.transactionLogs(logs)
                transactionSessionLogs.append(logs)
                print(transactionSessionLogs)
                fns.logging(
                    "info",
                    f"{current_user.username} has transacted data {fileName}")
                transData = pd.read_csv(f"./{tableConfig.project_name[0]}/" + fileName, low_memory=False)
                transDataHeaders = transData.columns.values.tolist()
                transDataBody = transData[:5].values.tolist()
                print(transDataHeaders)
                print(transDataBody)
                return render_template(
                    "transaction-data.html", transDataHeaders=transDataHeaders,
                    transDataBody=transDataBody,
                    projectName=tableConfig.project_name[0],
                    transactionFileList=transactionFileList,
                    transactionSessionLogs=transactionSessionLogs[::-1])
            except Exception as e:
                print("returned")
                logs = ["FAILED", fileName, date, time]
                fns.logging(
                    "warning",
                    f"{current_user.username} failed transacted data {fileName}"
                )
                return render_template(
                    "transaction-data.html",
                    projectName=tableConfig.project_name[0],
                    transactionFileList=transactionFileList,
                    transactionSessionLogs=transactionSessionLogs[::-1])
        except Exception as e:
            return render_template("404.html", error=e)
    else:
        print(transactionFileList)
        return render_template(
            "transaction-data.html",
            projectName=tableConfig.project_name[0],
            transactionFileList=transactionFileList,
            transactionSessionLogs=transactionSessionLogs[::-1])


@main.route('/save-data', methods=['GET', 'POST'])
def saveData():
    uploadCSVfile = request.files['uploadCSVfile']
    uploadCSVfile.save(f"./{tableConfig.project_name[0]}/" + uploadCSVfile.filename)
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
