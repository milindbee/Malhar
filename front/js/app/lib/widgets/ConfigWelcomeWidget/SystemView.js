/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var _ = require('underscore');
var Backbone = require('backbone');
var BaseView = require('./StepView');
var GatewayInfoModel = require('../../../../datatorrent/GatewayInfoModel');
var ConfigIPAddressCollection = require('./ConfigIPAddressCollection');
var ConfigPropertyModel = require('../../../../datatorrent/ConfigPropertyModel');
var Bbind = DT.lib.Bbindings;
var Notifier = DT.lib.Notifier;
var ConfigPropertyCollection = DT.lib.ConfigPropertyCollection;
var ConfigIssueCollection = DT.lib.ConfigIssueCollection;
var GatewayAddressModel = require('./GatewayAddressModel');
var HadoopLocationModel = require('../../../../datatorrent/HadoopLocationModel');
var DfsModel = require('./DfsModel');

var SystemView = BaseView.extend({

    events: {
        'click .continue': 'continue'
    },

    initialize: function(options) {
        BaseView.prototype.initialize.apply(this, arguments);
        this.dataSource = options.dataSource;
        this.navFlow = options.navFlow;

        this.error = false; //TODO
        this.loading = true;

        var hadoopLocationPromise = this.loadHadoopLocation();
        var aboutPromise = this.loadAbout();
        var ipListPromise = this.loadIPList();
        var defaultAddressPromise = this.loadDefaultAddress();
        var customAddressPromise = this.loadCustomAddress();
        var dfsPromise = this.loadDfsProperty();

        this.addressModel = new GatewayAddressModel();
        this.dfsModel = new DfsModel();

        var all = $.when(hadoopLocationPromise, aboutPromise, ipListPromise, customAddressPromise, defaultAddressPromise, dfsPromise);
        //var all = $.when(aboutPromise, customAddressPromise, defaultAddressPromise, dfsPromise);
        all.done(function () {
            var model;
            if (this.customAddressModel.get('ip') && this.customAddressModel.get('port')) {
                model = this.customAddressModel;
            } else {
                model = this.defaultAddressModel;
            }
            this.addressModel.init(model);

            this.dfsModel.init(this.dfsDirectory);

            this.loading = false;
            this.render();
        }.bind(this));

        all.fail(function () {
            //TODO there can be multiple errors
            this.error = true;
            this.render();
        }.bind(this));

        this.subview('hadoop-location', new Bbind.text({
            model: this.hadoopLocationModel,
            attr: 'value',
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error'
        }));

        this.subview('address-ip-input', new Bbind.text({
            model: this.addressModel,
            attr: 'ip',
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error'
        }));

        this.subview('address-port', new Bbind.text({
            model: this.addressModel,
            attr: 'port',
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error'
        }));

        this.ipSelectModel = new Backbone.Model({
            ip: null
        });
        this.subview('address-ip-select', new Bbind.select({
            model: this.ipSelectModel,
            attr: 'ip',
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error'
        }));
        this.listenTo(this.ipSelectModel, 'change', function () {
            var input = this.$el.find('.address-ip-input');
            var val = this.ipSelectModel.get('ip');
            if (val.length === 0) {
                input.val('');
                input.css('display', 'block');
                input.focus();
            } else {
                input.val(val);
                input.blur();
                input.css('display', 'none');
            }
        });

        this.subview('dfs-directory', new Bbind.text({
            model: this.dfsModel,
            attr: 'value',
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error'
        }));

        this.listenTo(this.hadoopLocationModel, 'change', this.inputChanged);
        this.listenTo(this.addressModel, 'change', this.inputChanged);
        this.listenTo(this.dfsModel, 'change', this.inputChanged);
    },

    inputChanged: function () {
        var hadoopLocationModelValid = this.hadoopLocationModel.isValid();
        var addressValid = this.addressModel.isValid();
        var dfsValid = this.dfsModel.isValid();

        if (hadoopLocationModelValid && addressValid && dfsValid) {
            this.$el.find('.continue').removeClass('disabled');
        } else {
            this.$el.find('.continue').addClass('disabled');
        }
    },

    loadDfsProperty: function () {
        var promise = this.loadProperty('dt.dfsRootDirectory');

        this.dfsDirectory = '';

        promise.then(function (data) {
            if (data && data.value) {
                this.dfsDirectory = data.value;
            }
        }.bind(this));

        return promise;
    },

    loadProperty: function (name) {
        var d = $.Deferred();

        var model = new ConfigPropertyModel({
            name: name
        });
        //TODO override fetchError: util.fetchError,

        var ajax = model.fetch();

        ajax.then(function (data) {
            d.resolveWith(null, [data]);
        }.bind(this));

        ajax.fail(function (jqXHR) {
            if (jqXHR.status === 404) {
                d.resolveWith(null, [null]);
            } else {
                //TODO
                //console.log(jqXHR.responseText);
                //var response = JSON.parse(jqXHR.responseText);
                //this.errorMsg = response.message;
                //this.errorMsg = 'Failed to load config property dt.attr.GATEWAY_ADDRESS';
                this.error = true;
                d.reject();
            }
        }.bind(this));

        return d.promise();
    },

    saveProperty: function (name, value) {
        var d = $.Deferred();

        var model = new ConfigPropertyModel({
            name: name,
            value: value
        });

        var ajax = model.save();

        ajax.done(function () {
            d.resolve();
        });

        ajax.fail(function (jqXHR) {
            if (jqXHR.status = 412) {
                var response = JSON.parse(jqXHR.responseText);
                this.errorMsg = response.message;
            } else {
                this.errorMsg = 'Failed to update property ' + propName;
            }

            d.rejectWith(null, [
                name,
                jqXHR
            ]);
        }.bind(this));

        return d.promise();
    },

    saveHadoopLocation: function () {
        var ajax = this.hadoopLocationModel.save();

        ajax.fail(function (jqXHR) {
            this.errorMsg = 'Failed to update hadoop location';
        }.bind(this));

        return ajax;
    },

    loadHadoopLocation: function () {
        var d = $.Deferred();

        this.hadoopLocationModel = new HadoopLocationModel();
        var ajax = this.hadoopLocationModel.fetch();

        ajax.done(function () {
            // save default value
            this.hadoopLocationModel.init(this.hadoopLocationModel.get('value'));
            d.resolve();
        }.bind(this));

        ajax.fail(function (jqXHR) {
            if (jqXHR.status === 404) { //TODO
                this.hadoopLocationModel.init('');
                d.resolve();
            } else {
                d.reject();
            }
        }.bind(this));

        return d.promise();
    },

    loadAbout: function () {
        var d = $.Deferred();

        this.about = new GatewayInfoModel({});
        this.about.fetch(); //TODO error handling

        this.listenTo(this.about, 'sync', function () {
            d.resolve();
        });

        return d.promise();
    },

    loadCustomAddress: function () {
        this.customAddressModel = new Backbone.Model({
            ip: '',
            port: ''
        });
        var promise = this.loadProperty('dt.attr.GATEWAY_ADDRESS');

        promise.then(function (data) {
            if (data && data.value) {
                var value = data.value;
                var parts = value.split(':');
                this.customAddressModel.set('ip', parts[0]);
                this.customAddressModel.set('port', parts[1]);
            }
        }.bind(this));

        return promise;
    },

    loadDefaultAddress: function () {
        this.defaultAddressModel = new Backbone.Model({
            ip: '',
            port: ''
        });
        var promise = this.loadProperty('dt.gateway.address');

        promise.then(function (data) {
            if (data && data.value) {
                var value = data.value;
                var parts = value.split(':');
                this.defaultAddressModel.set('ip', parts[0]);
                this.defaultAddressModel.set('port', parts[1]);
            }
        }.bind(this));

        return promise;
    },

    loadIPList: function () {
        var ajax = this.dataSource.getConfigIPAddresses();

        ajax.then(function (data) {
            this.ipAddresses = data.ipAddresses;
        }.bind(this));

        return ajax;
    },

    continue: function (event) {
        event.preventDefault();

        if (jQuery(event.target).hasClass('disabled')) {
            return;
        }

        this.$el.find('.address-ip-input').blur();
        this.$el.find('.address-port').blur();
        this.$el.find('.dfs-directory').blur();

        if (!this.hadoopLocationModel.isValid() || !this.addressModel.isValid() || !this.dfsModel.isValid()) {
            this.$el.find('.continue').addClass('disabled');
            return;
        }

        var hadoopLocationPromise;
        if (this.hadoopLocationModel.isChanged()) {
            hadoopLocationPromise = this.saveHadoopLocation();
        } else {
            hadoopLocationPromise = this.createResolvedPromise();
        }

        var addressPromise;
        if (this.addressModel.isChanged()) {
            addressPromise = this.saveProperty('dt.attr.GATEWAY_ADDRESS', this.addressModel.getValue());
        } else {
            addressPromise = this.createResolvedPromise();
        }

        // example values: /user/hadoop/DataTorrent, /user/hadoop/Stram
        var dfsPromise;
        if (this.dfsModel.isChanged()) {
            dfsPromise = this.saveProperty('dt.dfsRootDirectory', this.dfsModel.getValue());
        } else {
            dfsPromise = this.createResolvedPromise();
        }

        var all = $.when(hadoopLocationPromise, addressPromise, dfsPromise);

        all.done(function () {
            this.navFlow.go('SummaryView');
        }.bind(this));

        all.fail(function (propName) {
            this.render();
        }.bind(this));

        //jQuery(event.target).addClass('disabled');
    },

    createResolvedPromise: function () {
        var d = $.Deferred();
        d.resolve();
        return d.promise();
    },

    render: function() {
        var html = this.template({
            hadoopError: this.hadoopError,
            error: this.error,
            errorMsg: this.errorMsg,
            loading: this.loading,
            hadoopLocationModel: this.hadoopLocationModel,
            about: this.about,
            addressModel: this.addressModel,
            dfsModel: this.dfsModel,
            ipAddresses: this.ipAddresses
        });

        this.$el.html(html);

        if (this.assignments) {
            this.assign(this.assignments);
        }

        var selIP = this.addressModel.get('ip');
        if (selIP && _.indexOf(this.ipAddresses, selIP) >= 0) {
            _.defer(function () {
                this.$el.find('.address-ip-select').val(selIP);
            }.bind(this));
        } else {
            this.$el.find('.address-ip-input').show();
        }

        return this;
        //dt.attr.GATEWAY_ADDRESS ip:9090
        //dt.gateway.address
    },

    assignments: {
        '.hadoop-location': 'hadoop-location',
        '.address-ip-select': 'address-ip-select',
        '.address-ip-input': 'address-ip-input',
        '.address-port': 'address-port',
        '.dfs-directory': 'dfs-directory'
    }

});
exports = module.exports = SystemView;