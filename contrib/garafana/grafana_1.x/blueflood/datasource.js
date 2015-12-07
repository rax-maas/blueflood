    'use strict';
    define([
            'angular',
            'lodash',
            'kbn',
            './bluefloodReposeWrapper'
        ],
        function (angular, _, kbn) {
            //'use strict';

            var module = angular.module('grafana.services');

            module.factory('BluefloodDatasource', function ($q, $http, $location, templateSrv, ReposeAPI) {

                /**
                 * Datasource initialization. Calls when you refresh page, add
                 * or modify datasource.
                 *
                 * @param {Object} datasource Grafana datasource object.
                 */
                function BluefloodDatasource(datasource) {
                    this.name             = datasource.name;
                    this.type             = 'BluefloodDatasource';
                    this.username         = datasource.username;
                    this.apikey           = datasource.apikey;
                    this.tenantID         = datasource.tenantID;
                    this.useGraphite      = datasource.useGraphite

                    if(datasource.proxyEnabled && !datasource.useGraphite) {
                        this.url              = "http://"+$location.host()+":3000";
                        this.identityURL = "https://"+$location.host()+"/identity";
                    }
                    else if(!datasource.proxyEnabled && datasource.useGraphite){
                        this.url              = "graphite";
                    }
                    else {
                        this.identityURL = "https://identity.api.rackspacecloud.com/v2.0/tokens";
                        this.url              = datasource.url;
                    }

                    this.partials = datasource.partials || 'plugins/datasource/blueflood/partials';
                    this.annotationEditorSrc = this.partials + '/annotations.editor.html';

                    this.supportMetrics   = false;
                    this.supportAnnotations = true;

                    //Initialize Repose.
                    this.reposeAPI = new ReposeAPI(this.identityURL, this.username, this.apikey);
                }

                BluefloodDatasource.prototype.doAPIRequest = function(options, token) {
                    options.url   = this.url + '/v2.0/'+this.tenantID+options.url;
                    if(typeof token !== 'undefined'){
                        options.headers = {
                            'X-Auth-Token' : token.id
                        }
                    }
                    return $http.get(options.url, options);
                };

                /////////////////
                // Annotations //
                /////////////////

                BluefloodDatasource.prototype.annotationQuery = function (annotation, rangeUnparsed) {

                    var tags = templateSrv.replace(annotation.tags);
                    return this.events({range: rangeUnparsed, tags: tags})
                        .then(function (results) {
                            var list = [];
                            for (var i = 0; i < results.data.length; i++) {
                                var e = results.data[i];

                                if(this.useGraphite){
                                    list.push({
                                        annotation: annotation,
                                        time: e.when*1000,
                                        title: e.what,
                                        tags: e.tags,
                                        text: e.data
                                    });
                                }
                                else {
                                    list.push({
                                        annotation: annotation,
                                        time: e.when,
                                        title: e.what,
                                        tags: e.tags,
                                        text: e.data
                                    });
                                }
                            }
                            return list;
                        });
                };

                BluefloodDatasource.prototype.events = function (options) {
                    try {
                        var tags = '';
                        if (options.tags) {
                            tags = '&tags=' + options.tags;
                        }
                        var url = '';

                        if (this.useGraphite){
                            url = '/events/get_data?from=' + options.range.from +'&until=' +options.range.to
                        }
                        else{
                            url = '/events/getEvents?from=' +this.translateTime(options.range.from)+ '&until=' +this.translateTime(options.range.to) + tags
                        }

                        var d = $q.defer();
                        this.doAPIRequest({
                            method: 'GET',
                            url: url
                        }, this.reposeAPI.getToken()).then(function (response) {
                            if(response.status === 401){
                                this.doAPIRequest({
                                    method: 'GET',
                                    url: url
                                }, this.reposeAPI.getIdentity()).then(function (response) {
                                    if(response.status/100 === 4 || response.status === 500){
                                        alert("Error while connecting to Blueflood");
                                    }

                                    d.resolve(response);

                                });
                            }
                            else
                                d.resolve(response);
                        });
                    }
                    catch (err) {
                        d.reject(err);
                    }
                    return d.promise;
                };

                BluefloodDatasource.prototype.translateTime = function(date) {
                    return kbn.parseDate(date).getTime();
                };

                return BluefloodDatasource;
            });
        });