(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();
    var cols = [];
	var queryServerURL = "https://cosmosdbwebendpoint.azurewebsites.net/api/CosmosDBQuery?code=DpVgb0itRQPWp6yqTFGlFiPKozO2yqxMxi00YtVP5O4LfmkvAoDfkw==";
    // Define the schema
    myConnector.getSchema = function(schemaCallback) {

        var cosmosConnectionInfo = JSON.parse(tableau.connectionData),
        database = cosmosConnectionInfo.database,
        collection = cosmosConnectionInfo.collection
        
        // TODO: Change the below query to sampling random docs and extracting union of all columns
        query = "SELECT TOP 1 * from c";
		
        var queryInfo = {
            QueryText : query,
            Database: database,
            Collection: collection
        };

        var data = JSON.stringify(queryInfo);

        $.post(queryServerURL, data, function(resp) {
            tableData = [];
            console.log("Cosmos db result is " + resp);
            var respObj = JSON.parse(resp);
            var colNames = Object.keys(respObj[0]);
            for (var i = 0, len = colNames.length; i < len; i++) {
                // TODO: Expand below validation. Tableau columns support only characters, numbers and underscores
                //if(!colNames[i].includes(',') && !colNames[i].includes(' '))
                if(colNames[i] != ',')
                {
                    cols.push({
                        id : colNames[i],
                        dataType: tableau.dataTypeEnum.string
                    });
                }

            };
            var tableSchema = {
                id: "CosmosDbData",
                alias: "Data loaded from Cosmos DB",
                columns: cols
            };

            schemaCallback([tableSchema]);
        }, "json");
    };


    // Download the data
    myConnector.getData = function(table, doneCallback) {

        var cosmosConnectionInfo = JSON.parse(tableau.connectionData),
            database = cosmosConnectionInfo.database,
            collection = cosmosConnectionInfo.collection,
            query = cosmosConnectionInfo.query

        var queryInfo = {
            QueryText : query,
           Database: database,
           Collection: collection
        };

        var data = JSON.stringify(queryInfo);

        $.post(queryServerURL, data, function(resp) {
            tableData = [];
            console.log("Cosmos db result is " + resp);

            var respObj = JSON.parse(resp);
            // Iterate over the JSON object
            var tableData = [];

            for (var i = 0, len_i = respObj.length; i < len_i; i++) {
                var tableObj = {};
                for (var j = 0, len_j = cols.length; j < len_j; j++) {
                    tableObj[cols[j].id] = respObj[i][cols[j].id];
                    console.log(tableObj);
                }
                tableData.push(tableObj);
            }
        table.appendRows(tableData);
        doneCallback();
        }, "json");
    };

    tableau.registerConnector(myConnector);

     // Create event listeners for when the user submits the form
     $(document).ready(function() {
        $("#submitButton").click(function() {
			var dbcol = $("#dbcollist").val().trim().split("-");
			console.log(dbcol);
			var dbname = dbcol[0];
			var colname = dbcol[1];
			console.log("dbname->"+ dbname);
			console.log("colname->"+ colname);
            var cosmosConnectionInfo = {
				database: dbname,
                collection: colname,
                query: $("#custom-query").val().trim()
            };

            tableau.connectionData = JSON.stringify(cosmosConnectionInfo);
            tableau.connectionName = "Cosmos DB Reader"; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
	
	
	$(document).ready(function() {
        $("#btngetDBCollection").click(function() {
			queryServerURL ='https://cosmosdbwebendpoint.azurewebsites.net/api/CosmodbListDBCollections?code=aPIHSuCzv61PidRribKPCm6BzajPTK0FOSIZML65i1xEGbs3FqWSbw==';
			const request = new XMLHttpRequest();
			request.open('GET', queryServerURL, true);
			request.onload = function()
			{
					if (request.status === 200)
				{
					$("#dbcollist").children().remove().end()
					var resp = request.responseText;
					console.log(resp);
					var data = JSON.parse(resp);
					var i;
					for (i = 0; i < data.length; i++)
					{
					  var val= data[i].dbname + "-" + data[i].cname;
					  var opt = $("<option>").val(val).text(val);
					  $("#dbcollist").append(opt);
					}
				}
			}
			request.send(null);
        });
    });
	
	
	
	
	
	
	
})();