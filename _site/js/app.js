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





