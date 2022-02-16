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

#creating DB connection
conn = vae.connection()
cursor = conn.cursor()


#Fetch surveyNumber and dataSource from DB
def surveyNumber_dataSource():
    surveyNumberQuery = "select max(survey_id) from vestergaard_survey_master"
    cursor.execute(surveyNumberQuery)
    surveyNumber = cursor.fetchone()[0] + 1
    # print(surveyNumber, "surveyNumber")

    dataSourceQuery = "select * from vestergaard_datasource_master"
    cursor.execute(dataSourceQuery)
    dataSource = [i[1] for i in cursor.fetchall()]

    return surveyNumber, dataSource


def bulkUploadCSV(file, table):
    data = open(file, 'r')
    query = f"COPY {table} FROM STDIN DELIMITER ',' CSV HEADER"
    cursor.copy_expert(query, data)
    #commit changes
    conn.commit()
    #close the file
    data.close()


main = Blueprint('main', __name__)


#index page/login page
@main.route("/")
def login():
    return render_template("login.html")


#logout page
@main.route("/logout/")
def logout():
    return render_template("logout.html")


#dashboard page
@main.route("/dashboard/")
@login_required
def dashboard():
    return render_template("dashboard.html")


#fetches the data from the DB
@main.route("/fetch-data/", methods=['POST', 'GET'])
@login_required
def fetchData():
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

        surveyNumber, dataSourceList = surveyNumber_dataSource()

        #calling the extraction function
        vae.extraction(apiurl, startDate, endDate, fileType, surveyNumber,
                       dataSource)

        #make an entry into vestergaard_survey_master
        cursor.execute(
            """INSERT INTO vestergaard_survey_master (survey_id, survey_name, survey_country, survey_start_date, survey_end_date)
                        VALUES (%s, %s, %s, %s, %s)""",
            (surveyNumber, surveyName, dataSource, startDate, endDate))
        conn.commit()

        return render_template("fetch-data.html",
                               surveyNumber=surveyNumber + 1,
                               dataSource=dataSourceList)

    else:
        surveyNumber, dataSource = surveyNumber_dataSource()
        return render_template("fetch-data.html",
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

        if uploadFile.endswith(".csv"):
            #load the csv file into the staging table
            file = "./data/" + uploadFile
            bulkUploadCSV(file, "vestergaard_api_stg_data")

        elif uploadFile.endswith(".json"):
            #read the json file and convert it to a temp csv
            data = pd.read_json("./data/" + uploadFile)
            data = data.to_csv("./data/" + uploadFile + ".csv", index=False)

            #load the csv file into the staging table
            file = "./data/" + uploadFile + ".csv"
            bulkUploadCSV(file, "vestergaard_api_stg_data")

            #remove temp csv file
            os.remove("./data/" + uploadFile + ".csv")

        fileData.remove(uploadFile)
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
    return send_from_directory("./data", filename)


#deleting file
@main.route('/delete/<filename>', methods=['GET', 'POST'])
@login_required
def delete(filename):
    os.remove("./data/" + filename)
    return redirect(url_for('main.dataDirectory'))


@main.route('/master-data', methods=['GET', 'POST'])
@login_required
def masterData():
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
            print(schemaDownload)

            #download the schema
            query = f"select * from {schemaDownload} limit 0"
            cursor.execute(query)
            colnames = [colname[0] for colname in cursor.description]
            df = pd.DataFrame(columns=colnames)

            return Response(df.to_csv(index=False), mimetype='text/csv')

        except:
            schemaUpload = request.form['schemaUpload']
            CSVfile = request.files['CSVfile']
            CSVfile.save("./master_data/" + CSVfile.filename)
            # print(CSVfile.filename)
            file = "./master_data/" + CSVfile.filename
            bulkUploadCSV(file, schemaUpload)
            
            return render_template("master-data.html",
                                   tableDesc=tableDesc,
                                   fetchTable=fetchTable,
                                   uploadTable=uploadTable)
    else:
        return render_template("master-data.html",
                               tableDesc=tableDesc,
                               fetchTable=fetchTable,
                               uploadTable=uploadTable)


# we initialize our flask app using the __init__.py function
app = create_app()

#jinja zip for multiple iteration in html table
app.jinja_env.filters['zip'] = zip

if __name__ == '__main__':
    db.create_all(app=create_app())  # create the SQLite database
    app.run(port=8080, debug=True)  # run the flask app on debug mode
