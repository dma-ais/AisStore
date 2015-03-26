/**
 * Services that provides access to the AIS-Downloader backend
 */
angular.module('aisdownloader.app')


    .factory('AisQueryService', [ '$http', '$window',
        function($http, $window) {
        'use strict';

        var storage = $window.localStorage;
        var version = 1;

        return {

            /**
             * Resets the users' current parameters and client id
             */
            reset : function () {
                storage.clear();
                $window.location.reload();
            },


            /**
             * Returns the client id stored in the local storage.
             * If this does not exists, returns a new unique client id and stores it in the browser local storage
             */
            clientId : function () {
                var clientId = storage.clientId;
                if (!clientId) {
                    clientId = storage.clientId =
                        'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
                            .replace(/[xy]/g, function(c) {var r = Math.random()*16|0,v=c=='x'?r:r&0x3|0x8;return v.toString(16);});
                }
                return clientId;
            },


            /**
             * Gets existing parameters from the users local storage, or a new parameters entity
             */
            getSearchParams: function() {
                var params;
                try { params = JSON.parse(storage.params); } catch (e) {}
                if (!params || params.version != version) {
                    params = this.clearParams();
                }
                return params;
            },


            /**
             * Creates a new empty parameters entity with default values
             */
            clearParams: function () {
                return {
                    // Parametet version
                    version: version,

                    // Source Filter
                    sourceTxt: '', // Contains the visual search text
                    sourceType : '',
                    sourceBs : [],
                    sourceCountries : [],
                    sourceRegions : [],

                    // Target Filter
                    targetTxt: '', // Contains the visual search text
                    targetCountries : [],
                    targetImos : [],
                    targetNames : [],
                    targetCallsigns : [],
                    targetTypes : [],
                    targetNavstats : [],

                    // Message Filter
                    messageTxt: '', // Contains the visual search text
                    messageCountries : [],
                    messageMmsi : [],
                    messageImos : [],
                    messageNames : [],
                    messageCallsigns : [],
                    messageTypes : [],
                    messageNavstats : [],

                    // Advanced filter
                    advancedFilter: '',
                    filterType: { simple: true, advanced: false },

                    // MMSI
                    mmsi: undefined,

                    // Time Selection
                    startDate : moment().startOf('hour').valueOf(),
                    endDate : moment().startOf('hour').add(10, 'minutes').valueOf(),

                    // Area Selection
                    area : {
                        maxLat: undefined,
                        maxLon : undefined,
                        minLat: undefined,
                        minLon : undefined
                    },

                    // Output Format
                    outputFormat : 'raw',
                    outputTableFields: [ 'lat', 'lon', 'time', 'mmsi' ],
                    outputTableSeparator : ';',
                    outputTableHeader: true,

                    limit: undefined,
                    minDistance: undefined,
                    minDuration: undefined,
                    duplicateWindow: undefined
                }
            },


            /**
             * Saves the parameters in the local storage
             */
            saveSearchParams: function (params) {
                try { storage.params = JSON.stringify(params); } catch (e) {}
            },


            /**
             * Opens the given file
             */
            openFile: function(file) {
                window.open('/downloader/query/file/' + file.path);
            },

            /**
             * Schedules a query via the backend
             */
            execute: function(params, async, success, error) {
                $http.get('/downloader/query/execute/' + this.clientId() + '?async=' + async + '&params=' + encodeURIComponent(params))
                    .success(success)
                    .error(error);
            },


            /**
             * Deletes the given file in the client-specific download repository
             */
            deleteFile: function(file, success, error) {
                $http.get('/downloader/query/delete/' + file)
                    .success(success)
                    .error(error);
            },

            /**
             * Deletes the files in the client-specific download repository
             */
            deleteFiles: function(success, error) {
                $http.get('/downloader/query/delete-all/' + this.clientId())
                    .success(success)
                    .error(error);
            },

            /**
             * Lists all files in the client-specific download repository
             */
            listFiles: function(success, error) {
                $http.get('/downloader/query/list/' + this.clientId())
                    .success(success)
                    .error(error);
            },


            /**
             * Validates the AIS filter against the backend
             */
            validateFilter: function(filter, success, error) {
                $http.get('/downloader/query/validate-filter?filter=' + encodeURIComponent(filter))
                    .success(success)
                    .error(error);
            }
        };
    }]);

