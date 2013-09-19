angular.module('rssriver.service', [])
    .value('Data', {
        host: "http://localhost:9200",

	 riverName: "",
        Url: "",
  
	 settingsIndex: "",
        settingsType: "",
        settingsProxyHost: "",
        settingsProxyPort: 2402,
        settingsIncrementalDates: false,
        settingsStartDate: "",
        settingsMD5Names: true,
        settingsUpdateRate: 18000,
        settingsStartDate: "",



	 createRiverPath:  '',
 	 createRiverQuery:  '',
        query_index : '{ "type": "rss", "index": { "index": "',
        query_type : '", "type": "',
        query_proxyhost : '" }, "rss": {"proxyhost": "',
        query_proxyport: '", "proxyport": ',
 	 query_feedname: ', "feeds": [ { "name": "',
 	 query_url: '", "url": "',
	 query_incremental_dates: '", "update_rate": 18000, "incremental_dates": ',
	 query_end : ' } ] }}',
        elasticResponse: "",
        elasticError: [],
        mapping: {} ,

        tabs:['Feeds', 'New Feed', 'Settings']
    	}) 
	.value('Feed', {});


var app = angular.module('RssRiver', ['rssriver.service', 'ui.bootstrap', 'ui', 'ngSanitize']);

app.config(function ($routeProvider) {
    $routeProvider
        .when('/',
        {
            templateUrl: "views/feeds.html"
        })
	 .when('/feeds',
        {
            templateUrl: "views/feeds.html"
        })
        .when('/new feed',
        {
            templateUrl: "views/newfeed.html"
        })
	.when('/settings',
        {
            templateUrl: "views/settings.html"
        });
});





