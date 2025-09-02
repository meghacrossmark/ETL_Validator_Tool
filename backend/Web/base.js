const hostAddress="172.27.211.164"

let selectedSourceFile = null;
let selectedTargetFile = null;

document.getElementById("connectionType").addEventListener("change", function () {
    const connectionType = this.value;
    hideAll();
    document.getElementById("operationContainer").classList.remove("hidden");
    document.getElementById("performButton").style.backgroundColor = '#808080';

    if (connectionType === "local") {
        document.getElementById("sourceFileUploadContainer").classList.remove("hidden");
    } else if (connectionType === "MSSQL") {
         document.getElementById("mssqlFieldsContainer").classList.remove("hidden");
    } else if (connectionType === "databricks") {
        document.getElementById("databricksFieldsContainer").classList.remove("hidden");
    } else if (connectionType === "snowflake") {
        document.getElementById("snowflakeFieldsContainer").classList.remove("hidden");
    }
});

document.getElementById("operation").addEventListener("change", function () {
    const operation = this.value;
    const connectionType = document.getElementById("connectionType").value;

    // Reset hidden fields for operation
    document.getElementById("checkDuplicateContainer").classList.add("hidden");
    document.getElementById("columnContainer").classList.add("hidden");
    document.getElementById("targetFileUploadContainer").classList.add("hidden");
    document.getElementById("targetConnectionTypeContainer").classList.add("hidden");
	document.getElementById("mssqlFieldsContainer_target").classList.add("hidden");
	document.getElementById("databricksFieldsContainer_target").classList.add("hidden");
	document.getElementById("snowflakeFieldsContainerTarget").classList.add("hidden");

    if (operation === "check_duplicate") {
        document.getElementById("checkDuplicateContainer").classList.remove("hidden");
    } else if (operation === "check_count" || operation === "data_compare") {
        document.getElementById("targetConnectionTypeContainer").classList.remove("hidden");
    if(operation === "data_compare")
         document.getElementById("columnContainer").classList.remove("hidden");
    }
});

document.getElementById("targetConnectionType").addEventListener("change", function () {
    const targetConnectionType = this.value;

    // Reset all connection-specific fields
    document.getElementById("targetFileUploadContainer").classList.add("hidden");
    document.getElementById("mssqlFieldsContainer_target").classList.add("hidden");
    document.getElementById("databricksFieldsContainer_target").classList.add("hidden");
    document.getElementById("snowflakeFieldsContainerTarget").classList.add("hidden");

    if (targetConnectionType === "local") {
        document.getElementById("targetFileUploadContainer").classList.remove("hidden");
    } else if (targetConnectionType === "MSSQL") {
        document.getElementById("mssqlFieldsContainer_target").classList.remove("hidden");
    } else if (targetConnectionType === "databricks") {
        document.getElementById("databricksFieldsContainer_target").classList.remove("hidden");
    } else if (targetConnectionType === "snowflake") {
        document.getElementById("snowflakeFieldsContainerTarget").classList.remove("hidden");
    }
});

document.getElementById("btn_connect").addEventListener("click", function () {
    const connectionType = document.getElementById("connectionType").value;
    const operation = document.getElementById("operation").value;
    const detailReport = document.getElementById("detailReport").checked;
    const targetConnectionType = document.getElementById("targetConnectionType").value;
    const source_query = document.getElementById("queryInpSource")?.innerText.trim();
    const target_query = document.getElementById("queryInpTarget")?.innerText.trim();

    const params = {
        connectionType,
        operation,
        detailReport,
        targetConnectionType,
        // Get values for Source MSSQL
        source_serverName: document.getElementById("serverName")?.value,
        source_dbName: document.getElementById("dbName")?.value,
        source_table: document.getElementById("sourceTable")?.value,
        //Get values for Source Databricks
        source_tableDB: document.getElementById("sourceTableDB")?.value,
        source_serverHostName: document.getElementById("serverHostName")?.value,
        source_httpPath: document.getElementById("httpPath")?.value,
        source_accessToken: document.getElementById("accessToken")?.value,
        source_workspaceName: document.getElementById("workspaceName")?.value,
        source_dbSchemaName: document.getElementById("dbSchemaName")?.value,
        //Get values for Source snowFlake
        source_sf_username: document.getElementById("username_snowflake")?.value,
        source_sf_password: document.getElementById("password_snowflake")?.value,
        source_sf_account: document.getElementById("account_snowflake")?.value,
        source_sf_warehouse: document.getElementById("warehouse_snowflake")?.value,
        source_sf_database: document.getElementById("database_snowflake")?.value,
        source_sf_schema: document.getElementById("schema_snowflake")?.value,
        source_sf_tableName: document.getElementById("tableName_snowflake")?.value,
        //other Fields
        header: document.getElementById("header")?.value || 'all',
        selectColumn: document.getElementById("selectColumn")?.value || 'all',
        //Uploaded Files
        source_selectedFile: selectedSourceFile,
        source_seperator: document.getElementById("source_seperator").value,
        target_selectedFile: selectedTargetFile,
        target_seperator: document.getElementById("target_seperator").value,
        // Get values for Target MSSQL
        target_serverName: document.getElementById("targetServerName")?.value,
        target_dbName: document.getElementById("targetdbName")?.value,
        target_table: document.getElementById("targetTable")?.value,
        //Get values for Target Databricks
        target_tableDB: document.getElementById("targetTableDB")?.value,
        target_serverHostName: document.getElementById("targetServerHostName")?.value,
        target_httpPath: document.getElementById("targethttpPath")?.value,
        target_accessToken: document.getElementById("targetAccessToken")?.value,
        targe_workspaceName: document.getElementById("targetWorkspaceName")?.value,
        target_dbSchemaName: document.getElementById("targetdbSchemaName")?.value,
        //Get values for Target snowFlake
        target_sf_username: document.getElementById("username_snowflake_target")?.value,
        target_sf_password: document.getElementById("password_snowflake_target")?.value,
        target_sf_account: document.getElementById("account_snowflake_target")?.value,
        target_sf_warehouse: document.getElementById("warehouse_snowflake_target")?.value,
        target_sf_database: document.getElementById("database_snowflake_target")?.value,
        target_sf_schema: document.getElementById("schema_snowflake_target")?.value,
        target_sf_tableName: document.getElementById("tableName_snowflake_target")?.value,
        //queries
        source_query:source_query,
        target_query:target_query
    };

    document.getElementById("resultContainer").classList.remove("hidden");
    document.getElementById("resultHeader").innerText = "Connecting...";

    jsonParams = JSON.stringify(params);

    if (! (source_query.includes("SELECT") || target_query.includes("SELECT"))){
        fetch("http://"+hostAddress+":5000/connect", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
                },
            body: jsonParams
        })
        .then(response => response.json())
        .then(data => {
            document.getElementById("resultHeader").innerText = data.connection_status;
            document.getElementById("performButton").style.backgroundColor = '#4CAF50';
            document.getElementById("btn_connect").style.backgroundColor = '#808080';
            document.getElementById("sourceModal").style.backgroundColor = "#ffa500";
            if(targetConnectionType!='no_connection')
                document.getElementById("targetModal").style.backgroundColor = "#ffa500";
        })
        .catch(error => console.error("Error:", error));
    }else{
        fetch("http://"+hostAddress+":5000/updateTable", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
                },
            body: jsonParams
        })
        .then(response => response.json())
        .then(data => {
            document.getElementById("resultHeader").innerText = data.connection_status;
            document.getElementById("performButton").style.backgroundColor = '#4CAF50';
            document.getElementById("btn_connect").style.backgroundColor = '#808080';
            document.getElementById("btn_connect").textContent  = "Re-Loaded?"
        })
        .catch(error => console.error("Error:", error));
    }
});

document.getElementById("performButton").addEventListener("click", function () {
    document.getElementById("resultContainer").classList.remove("hidden");
    document.getElementById("resultHeader").innerText = "In Progress";

    fetch("http://"+hostAddress+":5000/perform_validation", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
            },
        body: jsonParams
    })
    .then(response => response.json())
    .then(data => {
        document.getElementById("resultHeader").innerText = data.result;
        document.getElementById("result").innerText = data.console_output;
        document.getElementById("performButton").style.backgroundColor = '#808080';
        document.getElementById("resetButton").style.backgroundColor = '#4CAF50';
        document.getElementById("sourceModal").style.backgroundColor = "#808080";
        document.getElementById("targetModal").style.backgroundColor = "#808080";
        hideAll();
    })
    .catch(error => console.error("Error:", error));
});

document.getElementById("resetButton").addEventListener("click", function () {
    localStorage.clear();
    fetch("http://"+hostAddress+":5000/reset", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
            }
    })
    .then(response => response.json())
    .then(data => {})
    .catch(error => console.error("Error:", error));
    location.reload();
});

document.getElementById("uploadFileButton").addEventListener("click", function () {
    const fileInput = document.createElement("input");
    fileInput.type = "file";
    fileInput.addEventListener("change", function () {
        selectedSourceFile = fileInput.files[0];

        const formData = new FormData();
        formData.append('source_file', selectedSourceFile);

        document.getElementById("uploadFileButton").textContent = "Uploading File...";
        document.getElementById("uploadFileButton").style.backgroundColor = "#737000";

        fetch("http://"+hostAddress+":5000/sourceFile", {
            method: "POST",
            body : formData
        })
        .then(response => response.json())
        .then(data => {
            document.getElementById("uploadFileButton").textContent = "Uploaded File: " +  data.source_fileName;
            document.getElementById("uploadFileButton").style.backgroundColor = "#808080";
            selectedSourceFile = data.source_fileName;
        })
        .catch(error => console.error("Error:", error));
    });
    fileInput.click();
});

document.getElementById("uploadTargetTableButton").addEventListener("click", function () {
    const fileInput = document.createElement("input");
    fileInput.type = "file";
    fileInput.addEventListener("change", function () {
        selectedTargetFile = fileInput.files[0];

        const formData = new FormData();
        formData.append('target_file', selectedTargetFile);

        document.getElementById("uploadTargetTableButton").textContent = "Uploading File...";
        document.getElementById("uploadTargetTableButton").style.backgroundColor = "#737000";

        fetch("http://"+hostAddress+":5000/targetFile", {
            method: "POST",
            body : formData
        })
        .then(response => response.json())
        .then(data => {
            document.getElementById("uploadTargetTableButton").textContent = "Uploaded File: " +  data.target_fileName;
            document.getElementById("uploadTargetTableButton").style.backgroundColor = "#808080";
            selectedTargetFile = data.target_fileName;
        })
        .catch(error => console.error("Error:", error));
    });
    fileInput.click();
});

var modal = document.getElementById('myModal');
var src_btn = document.getElementById('sourceModal');
var tgt_btn = document.getElementById('targetModal');
var span = document.getElementsByClassName('close')[0];
var iframe = document.getElementById('modalIframe');

tgt_btn.onclick = function() {
    let tableName = '';
    if (selectedTargetFile) {
        tableName = selectedTargetFile;  // or some other logic to get the source table name
    } else if (document.getElementById("targetTableDB")?.value) {
        tableName = document.getElementById("targetTableDB").value;
    } else if (document.getElementById("targetTable")?.value) {
        tableName = document.getElementById("targetTable").value;
    }
    localStorage.setItem('selectedTable', tableName);
    localStorage.setItem('queryType', "target");

    jsonParams = JSON.stringify({"connection_type": "target"});
    fetch("http://"+hostAddress+":5000/get_headers",{
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body:jsonParams
    })
    .then(response => response.json())
    .then(data => {
        localStorage.setItem('col_headers',data.headers);
        iframe.src = 'Web/query/builder.html';
        modal.style.display = "block";
    })
    .catch(error => {
        console.error("Error fetching data:", error);
    });
}

src_btn.onclick = function() {
    let tableName = '';
    if (selectedSourceFile) {
        tableName = selectedSourceFile;  // or some other logic to get the source table name
    } else if (document.getElementById("sourceTableDB")?.value) {
        tableName = document.getElementById("sourceTableDB").value;
    } else if (document.getElementById("sourceTable")?.value) {
        tableName = document.getElementById("sourceTable").value;
    }
    localStorage.setItem('selectedTable', tableName);
    localStorage.setItem('queryType', "source");

    jsonParams = JSON.stringify({"connection_type": "source"});
    fetch("http://"+hostAddress+":5000/get_headers",{
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body:jsonParams
    })
    .then(response => response.json())
    .then(data => {
        localStorage.setItem('col_headers',data.headers);
        iframe.src = 'Web/query/builder.html';
        modal.style.display = "block";
    })
    .catch(error => {
        console.error("Error fetching data:", error);
    });
}

span.onclick = function() {
     modal.style.display = "none";
     iframe.src = '';
     checkAndDisplayQuery();
}

window.onclick = function(event) {
    if (event.target == modal) {
        modal.style.display = "none";
        iframe.src = '';
        checkAndDisplayQuery();
    }
};

function checkAndDisplayQuery(){
    if (localStorage.getItem('source_query') || localStorage.getItem('target_query') ){
        document.getElementById("queryContainer").classList.remove("hidden");
        if (localStorage.getItem('source_query')){
            document.getElementById("queryContainerSource").classList.remove("hidden");
            document.getElementById("queryInpSource").innerText = localStorage.getItem('source_query');
        }
        if (localStorage.getItem('target_query')){
            document.getElementById("queryContainerTarget").classList.remove("hidden");
            document.getElementById("queryInpTarget").innerText = localStorage.getItem('target_query');
        }
        document.getElementById("btn_connect").textContent  = "Re-Load With Query?"
        document.getElementById("btn_connect").style.backgroundColor = '#4CAF50';
    }
}

function hideAll(){
    document.getElementById("sourceFileUploadContainer").classList.add("hidden");
    document.getElementById("mssqlFieldsContainer").classList.add("hidden");
    document.getElementById("databricksFieldsContainer").classList.add("hidden");
    document.getElementById("checkDuplicateContainer").classList.add("hidden");
    document.getElementById("columnContainer").classList.add("hidden");
    document.getElementById("snowflakeFieldsContainer").classList.add("hidden");
    //target Connection
    document.getElementById("targetFileUploadContainer").classList.add("hidden");
    document.getElementById("mssqlFieldsContainer_target").classList.add("hidden");
    document.getElementById("databricksFieldsContainer_target").classList.add("hidden");
    document.getElementById("snowflakeFieldsContainerTarget").classList.add("hidden");

    document.getElementById("queryContainer").classList.add("hidden");
}

    /*
    ======> Code to get current path and create command for execution <======
    const currentPath = window.location.pathname;
    let basePath = "";

    if (currentPath.startsWith('/')) {
	    basePath = currentPath.substring(1, currentPath.lastIndexOf('/') + 1).replace(/\//g, '\\');
		basePath = basePath.replace(/^\\/, '');
	} else {
	    basePath = currentPath.substring(0, currentPath.lastIndexOf("/") + 1);
    }
    console.log(currentPath);
    console.log("Base Path:", basePath);
    // Construct the Python command using the base path
    const pythonScriptPath = basePath + "methods\\Jenkins.py";
    const jsonParams = JSON.stringify(params);
    const command = `python ${pythonScriptPath} --params ${jsonParams}`;
    console.log(command); // Here you can send this command to your backend for execution
    */