function isUndefinedOrEmpty(value) {
    if (typeof value === "undefined")
        return true;
    if (!value)
        return true;
    return false;
}

function NewFeedSettingsCtrl($scope, $http, $filter, Data){
    $scope.data = Data;

    $scope.$watch('data.riverName', function(value){
        if ($scope.data.riverName === '') {
            $scope.data.riverNameMissing = true;
            return;
        }
        else{
            $scope.data.riverNameMissing = false;
        }
        $scope.setQuery();
    });


    $scope.$watch('data.Url', function(value) {
        if ($scope.data.Url === '') {
            $scope.data.UrlMissing = true;
            return;
        }
        else {
            if (isUndefinedOrEmpty($scope.data.Url)) {
                $scope.data.UrlMissing = true;
                return;
            }

            $scope.data.UrlMissing = false;

            if ($scope.data.settingsMD5Names) {
                var hash_code = md5($scope.data.Url);
                $scope.data.riverName = hash_code.substring(8);

            }
        }
        $scope.setQuery();
    });


    $scope.$watch('data.settingsMD5Names', function(value) {
        if ($scope.data.settingsMD5Names) {
            var riverNameInput = document.getElementById("riverName");
            riverNameInput.readonly = true;
            riverNameInput.disabled = true;
        }
        else {
            var riverNameInput = document.getElementById("riverName");
            riverNameInput.readonly = false;
            riverNameInput.disabled = false;

        }
        $scope.setQuery();
    });

 
    $scope.loadSettings = function() 
    {
		var path = $scope.data.host + "/_river/rss/_settings";
              $http.get(path)
            	   	.success(function(response){

			if (typeof (response._source) == 'object' && typeof (response._source.index) == 'object') {
				if (typeof (response._source.index.index) == 'string') {
					$scope.data.settingsIndex = response._source.index.index;
				}
				if (typeof (response._source.index.type) == 'string') {
					$scope.data.settingsType = response._source.index.type;
				}

			}
			if (typeof (response._source) == 'object' && typeof (response._source.rss) == 'object') {
				if (typeof (response._source.rss.proxyhost) == 'string') {
					$scope.data.settingsProxyHost = response._source.rss.proxyhost;
				}
				if (typeof (response._source.rss.proxyport) == 'number') {
					$scope.data.settingsProxyPort = response._source.rss.proxyport;
				}
				if (typeof (response._source.rss.incremental_dates) == 'boolean') {
					$scope.data.settingsIncrementalDates= response._source.rss.incremental_dates;
				}
				if (typeof (response._source.rss.md5_names) == 'boolean') {
					$scope.data.settingsMD5Names= response._source.rss.md5_names;
				}
				if (typeof (response._source.rss.update_rate) == 'number') {
					$scope.data.settingsUpdateRate= response._source.rss.update_rate;
				}
				if (typeof (response._source.rss.start_date) == 'string') {
					$scope.data.settingsStartDate= response._source.rss.start_date;
           	}
	        // set RSS river _meta request
	        $scope.setQuery();
  
			}
            })
            .error(function(data, status, headers, config){
                //console.log(data);

            });
    };

    $scope.saveSettings = function() {

        var path = $scope.data.host + '/_river/rss/_settings';
        var query = '{ ';
        query = query + ' "index": {';
        if (!isUndefinedOrEmpty($scope.data.settingsIndex)) {
            query = query + ' "index": "' + $scope.data.settingsIndex + '" ';
            if (!isUndefinedOrEmpty($scope.data.settingsType)) {
                query = query + ', "type": "' + $scope.data.settingsType + '" ';
            }
        }
        query = query + '} ';
        query = query + ', "rss": { "proxyhost": "' +  $scope.data.settingsProxyHost + '",  "proxyport": ' + $scope.data.settingsProxyPort+ ',  "update_rate": ' +  $scope.data.settingsUpdateRate + ',  "incremental_dates": ' + $scope.data.settingsIncrementalDates + ', "md5_names": ' +  $scope.data.settingsMD5Names  + '} '; 
        query = query + '} ';


        $http.put(path, query)
            .success(function(response) {
                $scope.data.settingsError = [];
                $scope.data.settingsResponse = response;
                console.log(response);
            })
            .error(function(data, status, headers, config) {
                $scope.data.settingsResponse = [];
                $scope.data.settingsValidation = [];
                if (status == '400')
                    $scope.data.settingsError = data;
                else {

                    $errorMessage = data.error.split(/(.*?)\[([\s\S]+)\]?/);
                    var errorData = { Error: "", details: [], rawError: "", status: status };

                    errordata.rawError = data;
                    errordata.Error = $errorMessage[1];

                    var nested = $errorMessage[2].split("nested:");

                    for (i in nested) {
                        var tempObject = {};
                        if (i == 0) {
                            tempObject.errorName = "Error";
                            tempObject.errorDesc = nested[i];
                            errordata.details.push(tempObject);
                        } else {
                            var nestedError = nested[i].split(/(.*?)\[([\s\S]+)\]?/);
                            tempObject.errorName = nestedError[1];
                            tempObject.errorDesc = nestedError[2];
                            errordata.details.push(tempObject);
                        }
                    }

                    $scope.data.settingsError = errorData;
                }

            });
    };

    $scope.setQuery = function() {

        $scope.data.createRiverPath = $scope.data.host + '/_river/' + $scope.data.riverName + '/_meta';

        $scope.data.createRiverQuery = '{ ';
        $scope.data.createRiverQuery += '  "type": "rss"'
        if (!isUndefinedOrEmpty($scope.data.settingsIndex)) {
            $scope.data.createRiverQuery += ', "index": {' + 
                                                 '  "index": "' + $scope.data.settingsIndex + '" ';
            if (!isUndefinedOrEmpty($scope.data.settingsType)) {
                $scope.data.createRiverQuery +=  ', "type": "' + $scope.data.settingsType + '" ';
            }
            $scope.data.createRiverQuery +=   '} ';
        }
        
        $scope.data.createRiverQuery += ', "rss": {  "proxyhost": "' +  $scope.data.settingsProxyHost + '"' +
                                  ', "proxyport": ' + $scope.data.settingsProxyPort + 
                                  ', "feeds": [ { ' +
                                        '  "name": "' + $scope.data.riverName + '"' +
                                        ', "url": "' + $scope.data.Url + '"' +
                                        ', "update_rate": ' +  $scope.data.settingsUpdateRate +
                                        ', "incremental_dates": ' + $scope.data.settingsIncrementalDates + 
                                        '} ]' + 
                                   '}';
        $scope.data.createRiverQuery +=  '} ';
        
        
				
    };

    $scope.createRiver = function() {

        $http.put($scope.data.createRiverPath, $scope.data.createRiverQuery)
            .success(function(response){
                $scope.data.createError = [];
                $scope.data.createResponse = response;
                console.log(response);
            })
            .error(function(data, status, headers, config){
                $scope.data.createResponse = [];
                $scope.data.createValidation = [];
                if (status == '400')
                    $scope.data.createError = data;
                else {

                    $errorMessage = data.error.split(/(.*?)\[([\s\S]+)\]?/);
                    var errorData = {Error: "", details: [], rawError : "", status: status};

                    errorData.rawError = data;
                    errorData.Error = $errorMessage[1];

                    var nested = $errorMessage[2].split("nested:");

                    for (i in nested){
                        var tempObject = {};
                        if (i == 0) {
                            tempObject.errorName = "Error";
                            tempObject.errorDesc = nested[i];
                            errorData.details.push(tempObject);
                        } else {
                            var nestedError = nested[i].split(/(.*?)\[([\s\S]+)\]?/);
                            tempObject.errorName = nestedError[1];
                            tempObject.errorDesc = nestedError[2];
                            errorData.details.push(tempObject);
                        }
                    }

                    $scope.data.createError = errorData;
                }

            });
    };

    // load saved settings from elasticsearch _river/rss/_settings document
      $scope.loadSettings();
  
}
