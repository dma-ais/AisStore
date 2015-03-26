/**
 * The main AIS-Downloader controller
 */
angular.module('aisdownloader.app')

    /**
     * The main AIS Query controller
     */
    .controller('AisQueryCtrl', ['$scope', '$timeout', 'growlNotifications', 'AisQueryService',
        function ($scope, $timeout, growlNotifications, AisQueryService) {
            'use strict';

            // ****************************************
            // ** Bootstrapping
            // ****************************************

            $scope.init = function () {

                $scope.params = AisQueryService.getSearchParams();

                // The visual search fields reside in tabs, so, the dom is not ready it seems...
                $timeout(function () {
                    // Initialize source filter
                    $scope.sourceFilter = VS.init({
                        container: $('#sourceFilter'),
                        query: '',
                        showFacets: true,
                        unquotable: [],
                        callbacks: {
                            search: function (query, searchCollection) {
                                $scope.$apply(function () {
                                    $scope.updateSourceFilter(true);
                                });
                            },
                            facetMatches: function (callback) {
                                callback(['type', 'bs', 'country', 'region'], {preserveOrder: true});
                            },
                            valueMatches: function (facet, searchTerm, callback) {
                                switch (facet) {
                                    case 'type':
                                        callback(sourceTypes, {preserveOrder: true});
                                        break;
                                    case 'country':
                                        callback(countryList, {preserveOrder: true});
                                        break;
                                }
                            }
                        }
                    });

                    // Initialize target filter
                    $scope.targetFilter = VS.init({
                        container: $('#targetFilter'),
                        query: '',
                        showFacets: true,
                        unquotable: [],
                        callbacks: {
                            search: function (query, searchCollection) {
                                $scope.$apply(function () {
                                    $scope.updateTargetFilter(true);
                                });
                            },
                            facetMatches: function (callback) {
                                callback(['country', 'imo', 'callsign', 'navstat', 'name', 'type'], {preserveOrder: true});
                            },
                            valueMatches: function (facet, searchTerm, callback) {
                                switch (facet) {
                                    case 'country':
                                        callback(countryList, {preserveOrder: true});
                                        break;
                                    case 'type':
                                        callback(shipTypes, {preserveOrder: true});
                                        break;
                                    case 'navstat':
                                        callback(navstatValues, {preserveOrder: true});
                                        break;
                                }
                            }
                        }
                    });

                    // Initialize message filter
                    $scope.messageFilter = VS.init({
                        container: $('#messageFilter'),
                        query: '',
                        showFacets: true,
                        unquotable: [],
                        callbacks: {
                            search: function (query, searchCollection) {
                                $scope.$apply(function () {
                                    $scope.updateMessageFilter(true);
                                });
                            },
                            facetMatches: function (callback) {
                                callback(['country', 'mmsi', 'imo', 'callsign', 'navstat', 'name', 'type'], {preserveOrder: true});
                            },
                            valueMatches: function (facet, searchTerm, callback) {
                                switch (facet) {
                                    case 'country':
                                        callback(countryList, {preserveOrder: true});
                                        break;
                                    case 'type':
                                        callback(shipTypes, {preserveOrder: true});
                                        break;
                                    case 'navstat':
                                        callback(navstatValues, {preserveOrder: true});
                                        break;
                                }
                            }
                        }
                    });

                    $scope.sourceFilter.searchBox.value($scope.params.sourceTxt || '');
                    $scope.targetFilter.searchBox.value($scope.params.targetTxt || '');
                    $scope.messageFilter.searchBox.value($scope.params.messageTxt || '');
                }, 100);

            };


            // ****************************************
            // ** Advanced Filtering
            // ****************************************

            /**
             * Called when the simple/advanced tabs are clicked
             * @param useAdvancedFilter whether the simple or advanced tab is clicked
             */
            $scope.useAdvancedFilter = function (useAdvancedFilter) {
                // The weird modelling of the filter type is due to this bug:
                // https://github.com/angular-ui/bootstrap/issues/611
                $scope.params.filterType.simple = !useAdvancedFilter;
                $scope.params.filterType.advanced = useAdvancedFilter;
            };

            // ****************************************
            // ** Source Filtering
            // ****************************************

            /**
             * Called when the source filter has been updated
             */
            $scope.updateSourceFilter = function (updateDownloadUrl) {
                // Reset filter params
                $scope.params.sourceTxt = $scope.sourceFilter.searchBox.value();
                $scope.params.sourceType = '';
                $scope.params.sourceBs = [];
                $scope.params.sourceCountries = [];
                $scope.params.sourceRegions = [];

                var facets = $scope.sourceFilter.searchQuery.facets();
                for (var f in facets) {
                    var facet = facets[f];
                    if (facet.type) {
                        $scope.params.sourceType = facet.type;
                    } else if (facet.bs) {
                        $scope.params.sourceBs.push(facet.bs);
                    } else if (facet.country) {
                        $scope.params.sourceCountries.push(facet.country);
                    } else if (facet.region) {
                        $scope.params.sourceRegions.push(facet.region);
                    }
                }

                if (updateDownloadUrl) {
                    $scope.updateDownloadUrl();
                }
            };

            // ****************************************
            // ** Target Filtering
            // ****************************************

            /**
             * Called when the target filter has been updated
             */
            $scope.updateTargetFilter = function (updateDownloadUrl) {
                // Reset filter params
                $scope.params.targetTxt = $scope.targetFilter.searchBox.value();
                $scope.params.targetCountries = [];
                $scope.params.targetImos = [];
                $scope.params.targetCallsigns = [];
                $scope.params.targetNavstats = [];
                $scope.params.targetNames = [];
                $scope.params.targetTypes = [];

                var facets = $scope.targetFilter.searchQuery.facets();
                for (var f in facets) {
                    var facet = facets[f];
                    if (facet.country) {
                        $scope.params.targetCountries.push(facet.country);
                    } else if (facet.imo) {
                        $scope.params.targetImos.push(facet.imo);
                    } else if (facet.callsign) {
                        $scope.params.targetCallsigns.push(facet.callsign);
                    } else if (facet.navstat) {
                        $scope.params.targetNavstats.push(facet.navstat);
                    } else if (facet.name) {
                        $scope.params.targetNames.push(facet.name);
                    } else if (facet.type) {
                        $scope.params.targetTypes.push(facet.type);
                    }
                }

                if (updateDownloadUrl) {
                    $scope.updateDownloadUrl();
                }
            };

            // ****************************************
            // ** Message Filtering
            // ****************************************

            /**
             * Called when the message filter has been updated
             */
            $scope.updateMessageFilter = function (updateDownloadUrl) {
                // Reset filter params
                $scope.params.messageTxt = $scope.messageFilter.searchBox.value();
                $scope.params.messageCountries = [];
                $scope.params.messageMmsi = [];
                $scope.params.messageImos = [];
                $scope.params.messageCallsigns = [];
                $scope.params.messageNavstats = [];
                $scope.params.messageNames = [];
                $scope.params.messageTypes = [];

                var facets = $scope.messageFilter.searchQuery.facets();
                for (var f in facets) {
                    var facet = facets[f];
                    if (facet.country) {
                        $scope.params.messageCountries.push(facet.country);
                    } else if (facet.mmsi) {
                        $scope.params.messageMmsi.push(facet.mmsi);
                    } else if (facet.imo) {
                        $scope.params.messageImos.push(facet.imo);
                    } else if (facet.callsign) {
                        $scope.params.messageCallsigns.push(facet.callsign);
                    } else if (facet.navstat) {
                        $scope.params.messageNavstats.push(facet.navstat);
                    } else if (facet.name) {
                        $scope.params.messageNames.push(facet.name);
                    } else if (facet.type) {
                        $scope.params.messageTypes.push(facet.type);
                    }
                }

                if (updateDownloadUrl) {
                    $scope.updateDownloadUrl();
                }
            };

            // ****************************************
            // ** Time interval
            // ****************************************

            $scope.$watch(
                function () {
                    return $scope.params.startDate;
                },
                function (a) {
                    $scope.validateTimeInterval(true);
                },
                true);

            $scope.$watch(
                function () {
                    return $scope.params.endDate;
                },
                function (a) {
                    $scope.validateTimeInterval(false);
                },
                true);

            $scope.validateTimeInterval = function (startDateUpdated) {

                if (startDateUpdated && $scope.params.startDate > $scope.params.endDate) {
                    $scope.params.endDate = moment($scope.params.startDate).add(10, 'minutes').valueOf()
                } else if (!startDateUpdated && $scope.params.startDate > $scope.params.endDate) {
                    $scope.params.startDate = moment($scope.params.endDate).add(-10, 'minutes').valueOf()
                }

                $scope.updateDownloadUrl();
            };


            // ****************************************
            // ** Area Filtering
            // ****************************************

            $scope.showAreaMap = false;

            /**
             * Called when the area has been updated
             * @param coordinates the new coordinates
             */
            $scope.updateArea = function (coordinates) {
                function max(m1, m2) {
                    return (m2 === undefined) ? m1 : Math.max(m1, m2);
                }

                function min(m1, m2) {
                    return (m2 === undefined) ? m1 : Math.min(m1, m2);
                }

                $scope.params.area.maxLat = $scope.params.area.maxLon = $scope.params.area.minLat = $scope.params.area.minLon = undefined;
                if (coordinates && coordinates.length > 0) {
                    for (var c in coordinates) {
                        var latLon = coordinates[c];
                        $scope.params.area.maxLat = max(latLon[1], $scope.params.area.maxLat);
                        $scope.params.area.maxLon = max(latLon[0], $scope.params.area.maxLon);
                        $scope.params.area.minLat = min(latLon[1], $scope.params.area.minLat);
                        $scope.params.area.minLon = min(latLon[0], $scope.params.area.minLon);
                    }
                }
            };

            /**
             * Clears the selected area
             */
            $scope.clearArea = function () {
                $scope.updateArea(undefined);
            };

            /**
             * Returns if the area is well-defined
             * @returns {*}
             */
            $scope.areaDefined = function () {
                var a = $scope.params.area;
                return a.maxLat && a.maxLon && a.minLat && a.minLon;
            };

            // ****************************************
            // ** Output Format
            // ****************************************

            $scope.$watch(
                function () {
                    return $scope.params.outputFormat;
                },
                function (a) {
                    $scope.updateDownloadUrl();
                },
                true);

            $scope.outputTableFields = outputTableFields;

            // ****************************************
            // ** Downloading
            // ****************************************

            $scope.downloadUrl = '';

            function encodeValues(values) {
                var enc = [];
                for (var v in values) {
                    enc.push(encodeURIComponent(values[v]));
                }
                return enc;
            }

            /**
             * Updates the download URL
             */
            $scope.updateDownloadUrl = function () {

                var url = 'interval=' + moment($scope.params.startDate).utc().format('YYYY-M-DTHH:mm:ss') + 'Z'
                    + '/' + moment($scope.params.endDate).utc().format('YYYY-M-DTHH:mm:ss') + 'Z';

                // Simple filtering
                if ($scope.params.filterType.simple) {
                    var filter = '';

                    // Source filter
                    if ($scope.params.sourceType == 'SAT' || $scope.params.sourceType == 'LIVE') {
                        filter += ' & s.type=' +$scope.params.sourceType;
                    }
                    if ($scope.params.sourceBs.length > 0) {
                        filter += ' & s.bs=' + encodeValues($scope.params.sourceBs).join();
                    }
                    if ($scope.params.sourceCountries.length > 0) {
                        filter += ' & s.country=' + $scope.params.sourceCountries.join();
                    }
                    if ($scope.params.sourceRegions.length > 0) {
                        filter += ' & s.region=' + encodeValues($scope.params.sourceRegions).join();
                    }

                    // Target filter
                    if ($scope.params.targetCountries.length > 0) {
                        filter += ' & t.country=' + $scope.params.targetCountries.join();
                    }
                    if ($scope.params.targetImos.length > 0) {
                        filter += ' & t.imo=' + encodeValues($scope.params.targetImos).join();
                    }
                    if ($scope.params.targetNames.length > 0) {
                        filter += ' & t.name=' + encodeValues($scope.params.targetNames).join();
                    }
                    if ($scope.params.targetCallsigns.length > 0) {
                        filter += ' & t.cs=' + encodeValues($scope.params.targetCallsigns).join();
                    }
                    if ($scope.params.targetTypes.length > 0) {
                        filter += ' & t.type=' + $scope.params.targetTypes.join();
                    }
                    if ($scope.params.targetNavstats.length > 0) {
                        filter += ' & t.navstat=' + encodeValues($scope.params.targetNavstats).join();
                    }

                    // Message filter
                    if ($scope.params.messageCountries.length > 0) {
                        filter += ' & m.country=' + $scope.params.messageCountries.join();
                    }
                    if ($scope.params.messageMmsi.length > 0) {
                        filter += ' & m.mmsi=' + encodeValues($scope.params.messageMmsi).join();
                    }
                    if ($scope.params.messageImos.length > 0) {
                        filter += ' & m.imo=' + encodeValues($scope.params.messageImos).join();
                    }
                    if ($scope.params.messageNames.length > 0) {
                        filter += ' & m.name=' + encodeValues($scope.params.messageNames).join();
                    }
                    if ($scope.params.messageCallsigns.length > 0) {
                        filter += ' & m.cs=' + encodeValues($scope.params.messageCallsigns).join();
                    }
                    if ($scope.params.messageTypes.length > 0) {
                        filter += ' & m.type=' + $scope.params.messageTypes.join();
                    }
                    if ($scope.params.messageNavstats.length > 0) {
                        filter += ' & m.navstat=' + encodeValues($scope.params.messageNavstats).join();
                    }

                    if (filter.length > 0) {
                        url += '&filter=' + encodeURIComponent(filter.substring(3));
                    }

                } else if ($scope.params.filterType.advanced && $scope.params.advancedFilter) {
                    url += '&filter=' + encodeURIComponent($scope.params.advancedFilter);
                }

                if ($scope.params.mmsi) {
                    var mmsi = $scope.params.mmsi.replace(/,/g, ' ').split(" ");
                    for (var m in mmsi) {
                        if (mmsi[m]) {
                            url += '&mmsi=' + mmsi[m].trim();
                        }
                    }
                }

                if ($scope.areaDefined()) {
                    var a = $scope.params.area;
                    url += '&box=' + a.maxLat.toFixed(3) + ',' + a.minLon.toFixed(3) + ','
                    + a.minLat.toFixed(3) + ',' + a.maxLon.toFixed(3);
                }

                if ($scope.params.outputFormat != 'raw') {
                    url += '&output=' + $scope.params.outputFormat;
                }
                if ($scope.params.outputFormat == 'table') {
                    url += '&columns=' + $scope.params.outputTableFields.join(';');
                    url += '&separator=' + $scope.params.outputTableSeparator;
                    if (!$scope.params.outputTableHeader) {
                        url += '&noHeader';
                    }
                }

                if ($scope.params.limit) {
                    url += '&limit=' + $scope.params.limit;
                }
                if ($scope.params.minDistance) {
                    url += '&minDistance=' + $scope.params.minDistance;
                }
                if ($scope.params.minDuration) {
                    url += '&minDuration=P0Y0M0DT0H0M' + $scope.params.minDuration + 'S'; // NB: ISOPeriodFormat
                }
                if ($scope.params.duplicateWindow) {
                    url += '&duplicateWindow=' + $scope.params.duplicateWindow;
                }

                $scope.downloadUrl = url;
            };

            /**
             * Update the model from the filters and updates the download URL
             */
            $scope.recomputeDownloadUrl = function () {
                $scope.updateSourceFilter(false);
                $scope.updateTargetFilter(false);
                $scope.updateMessageFilter(false);
                $scope.updateDownloadUrl();
            };

            $scope.downloadDisabled = false;

            /**
             * Main search method
             */
            $scope.execute = function (async) {
                $scope.recomputeDownloadUrl();
                $scope.downloadDisabled = true;

                AisQueryService.execute(
                    $scope.downloadUrl,
                    async,
                    function (result) {
                        if (!async) {
                            AisQueryService.openFile(result);
                        }
                    },
                    function () {});

                AisQueryService.saveSearchParams($scope.params);
                growlNotifications.add(async ? 'Download Scheduled' : 'Download Scheduled.<br><small>Opens upon completion</small>', 'info', 2000);

                // Re-enable the Download button after 2 seconds to avoid double-clicks
                $timeout(function () {
                    $scope.downloadDisabled = false;
                }, 2000);
            };

            $scope.clear = function () {
                $scope.params = AisQueryService.clearParams();
                $scope.sourceFilter.searchBox.value($scope.params.sourceTxt || '');
                $scope.targetFilter.searchBox.value($scope.params.targetTxt || '');
                $scope.messageFilter.searchBox.value($scope.params.messageTxt || '');
            };

            $scope.reset = function () {
                AisQueryService.reset();
            };

            /**
             * Open dialog with URL
             */
            $scope.openInAisStore = function () {
                $scope.recomputeDownloadUrl();
                var url = 'https://ais2.e-navigation.net/aisview/rest/store/query?' + $scope.downloadUrl;
                window.open(url);
            };

            /**
             * Open dialog with URL
             */
            $scope.copy = function () {
                $scope.recomputeDownloadUrl();
                window.prompt("Copy to clipboard:",
                    'https://ais2.e-navigation.net/aisview/rest/store/query?' + $scope.downloadUrl);
            }
        }])


    /**
     * The main AIS Download Results controller
     */
    .controller('AisResultsCtrl', ['$scope', '$interval', 'AisQueryService',
        function ($scope, $interval, AisQueryService) {
            'use strict';

            $scope.badgeColor = 'blue';
            $scope.resultsContent = '';
            $scope.files = [];
            $scope.clientId = AisQueryService.clientId();

            // Refresh the list of files every 5 seconds
            $interval(function () { $scope.updateFiles(); }, 5000);

            /**
             * Updates the list of files from the backend
             */
            $scope.updateFiles = function () {
                AisQueryService.listFiles(
                    function(files) {
                        $scope.files = files;
                        $scope.badgeColor = 'blue';
                        for (var f in $scope.files) {
                            if (!$scope.files[f].complete) {
                                $scope.badgeColor = 'red';
                            }
                        }
                    },
                    function (err) {
                        console.error("Error fetching files " + err);
                    });
            };

            // Initial loading of files
            $scope.updateFiles();

            /**
             * Opens the given file
             * @param file the file to open
             */
            $scope.openFile = function (file) {
                AisQueryService.openFile(file);
            };

            /**
             * Deletes the given file
             * @param file the file to delete
             */
            $scope.deleteFile = function (file) {
                AisQueryService.deleteFile(
                    file.path,
                    function () { $scope.updateFiles(); },
                    function () { $scope.updateFiles(); });
            };

            /**
             * Deletes all files
             */
            $scope.deleteFiles = function () {
                AisQueryService.deleteFiles(
                    function () { $scope.updateFiles(); },
                    function () { $scope.updateFiles(); });
            }

        }]);
