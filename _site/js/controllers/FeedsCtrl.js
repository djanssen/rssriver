
/**
 * Misc functions
 */

function isUndefined(value) {
    return typeof value === "undefined";
}

function isNumber(n) {
    return !isNaN(parseFloat(n)) && isFinite(n);
}



function FeedsCtrl($scope, $http, Feed, Data){
    $scope.feed = Feed;
    $scope.data = Data;



    /**
     * Detection section
     *
     * These functions detect mappings, fields, etc
     */


    	$scope.detect = function() 
	{
       	var path = $scope.data.host + "/_river/_search?size=10000";

	        $http.get(path)
            	   	.success(function(response){
		       	console.log(response);

		             //process  _meta
                detectFeeds(response);

		// 
		detectStatus(response);

		 // process _lastupdated
		 detectLastUpdated(response);


            })
            .error(function(data, status, headers, config){
                //console.log(data);

            });
    }


    function detectFeeds(response) {

        //This section loops over all the 'hits' objects and converts
        //the long "path-like" parameters into objects - kludgy...probably a better way to do this
        $scope.feed.Feeds = {};

 	 for (i in response.hits.hits) {
 		if (response.hits.hits[i]._id == "_meta") {
		     var river_name = response.hits.hits[i]._type;
	            $scope.feed.Feeds[river_name] = {};
       	     for (j in response.hits.hits[i]._source.rss.feeds) {
		     	var feed_name = response.hits.hits[i]._source.rss.feeds[j].name;
	           	$scope.feed.Feeds[river_name][feed_name ] = {};
		       $scope.feed.Feeds[river_name][feed_name ].index = response.hits.hits[i]._source.index;
	 	       $scope.feed.Feeds[river_name][feed_name ].type = response.hits.hits[i]._source.type;
	              $scope.feed.Feeds[river_name][feed_name ].properties = response.hits.hits[i]._source.rss.feeds[j];
       	     }
		}
        }
    }


 function detectLastUpdated(response) {

        //This section loops over all the 'hits' objects and converts
        //the long "path-like" parameters into objects - kludgy...probably a better way to do this
    	 for (i in response.hits.hits) {
		var prefix = "_lastupdated_";
 		if (response.hits.hits[i]._id.lastIndexOf(prefix , 0) == 0) {
		     var river_name = response.hits.hits[i]._type;
	            var feed_name  = response.hits.hits[i]._id.substring(prefix.length);
	            $scope.feed.Feeds[river_name][feed_name].lastupdated =  response.hits.hits[i]._source.rss;
		}
        }
    }



 function detectStatus(response) {

 	 for (i in response.hits.hits) {
 		if (response.hits.hits[i]._id == "_status") {
		     	var river_name = response.hits.hits[i]._type;
		  	for (feed_name in $scope.feed.Feeds[river_name]) {
				$scope.feed.Feeds[river_name][feed_name].node = response.hits.hits[i]._source.node;
			}
		}
	}
}



    //Begin the actual detection
    $scope.detect();




    /**
     * Delet Feed 
     *
     * These functions monitor changes in text or feeds and update the view
     */
	$scope.deleteRiver = function(river_name) {

   		var path = $scope.data.host + "/_river/" + river_name;

	       $http.delete(path)
            .success(function(response){
                console.log(response);

            })
            .error(function(data, status, headers, config){
                //console.log(data);

            });
	};


}
