// Copyright 2021 Google LLC

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

function doGet() {
    bq_json = exportSheetAsJSON()
    return ContentService.createTextOutput(bq_json);
  }


  function showMessageBox(params) {
    var params = {
    BUCKET_NAME: 'td2bq-mapper-storage',
    FILE_PATH: 'td2bq_permissions_map.json',
    ACCESS_TOKEN: 'None',
    PROJECT_NAME: 'td2bq-permissions-mapper',
    };

    params.ACCESS_TOKEN = getAccessToken(params);

    if(params.ACCESS_TOKEN == "None"){
      Logger.log("No access token provided. Not uploading json file to GCS bucket")
    }
    else{
      //provided some string as access token.
      uploadFileToGCS(params)
    }
  }


  function getAccessToken(params) {
    var access_token = "None"
    var ui = SpreadsheetApp.getUi();
    var mypromt = "Type in the Access Token for Project: "+ params.PROJECT_NAME;
    Logger.log(mypromt);

    var response = ui.prompt(mypromt,"use: gcloud auth print-access-token", ui.ButtonSet.OK_CANCEL);

    // Process the user's response.
    if (response.getSelectedButton() == ui.Button.OK) {
      Logger.log('Access token entered');
      access_token = response.getResponseText();
    } else if (response.getSelectedButton() == ui.Button.CANCEL) {
      Logger.log('The user didn\'t want to provide an access token.');
      access_token = "None"
    } else {
      Logger.log('The user clicked the close button in the dialog\'s title bar.');
      access_token = "None"
    }
    return(access_token)
  }

  function uploadFileToGCS(params) {
    bq_json = exportSheetAsJSON()

    var blob = Utilities.newBlob(bq_json)
    var bytes = blob.getBytes();

    var url = 'https://www.googleapis.com/upload/storage/v1/b/BUCKET/o?uploadType=media&name=FILE'
      .replace("BUCKET", params.BUCKET_NAME)
      .replace("FILE", encodeURIComponent(params.FILE_PATH));

    var response = UrlFetchApp.fetch(url, {
      method: "POST",
      contentLength: bytes.length,
      contentType: blob.getContentType(),
      payload: bytes,
      headers: {
        Authorization: 'Bearer ' + params.ACCESS_TOKEN //ScriptApp.getOAuthToken()
      }
    });

    var result = JSON.parse(response.getContentText());
    Logger.log(JSON.stringify(result, null, 2));
  }


  function exportSheetAsJSON() {
    var sheet = SpreadsheetApp.getActiveSheet();
    var rows = sheet.getDataRange();
    var numRows = rows.getNumRows();
    var numCols = rows.getNumColumns();
    var values = rows.getValues();

    var output = "";
    //output += "{\""+sheet.getName()+"\" : {\n";
    output += "{"
    var version = values[0][1]
    output += JSON.stringify("version") +": "+ JSON.stringify(version) + ",\n"
    Logger.log(version);

    //var header = values[1];
    // set headers to something with out spaces
    var header = ["arc", "description", "bq_permissions"]
    Logger.log(header)

    for (var i = 2; i < numRows; i++) {
      if (i > 2) output += " , \n";
      var row = values[i];
      output += JSON.stringify(row[0]) + ": {"
      for (var a = 1;a<numCols;a++){
        if (a > 1) output += " , ";
          if(a == 2) {
            //deal with BQ permissions
            var bq_perm = row[a].split("\n");
            var bq_perm = JSON.stringify(bq_perm)
            output += JSON.stringify(header[a]) + ": " + bq_perm;
          }
          else {
            output += JSON.stringify(header[a]) + ": " + JSON.stringify(row[a]);
          }
      }
      output += "}";
      //Logger.log(row);
    }
    output += "\n}";
    Logger.log(output);

    return(output)
  };


