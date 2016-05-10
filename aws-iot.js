/**
 * Copyright 2013, 2016 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

module.exports = function(RED) {
    "use strict";
    var awsIot = require('aws-iot-device-sdk');    
    var util = require("util");
    var isUtf8 = require('is-utf8');

    function matchTopic(ts,t) {
        if (ts == "#") {
            return true;
        }
        var re = new RegExp("^"+ts.replace(/([\[\]\?\(\)\\\\$\^\*\.|])/g,"\\$1").replace(/\+/g,"[^/]+").replace(/\/#$/,"(\/.*)?")+"$");
        return re.test(t);
    }

    function AWS_IOTBrokerNode(n) {
        RED.nodes.createNode(this,n);

        // Configuration options passed by Node Red
        this.broker = n.broker;
        this.port = n.port;
        this.clientid = n.clientid;
        this.keepalive = n.keepalive;

        // Config node state
        this.brokerurl = "";
        this.connected = false;
        this.connecting = false;
        this.closing = false;
        this.options = {};
        this.queue = [];
        this.subscriptions = {};
        
        this.keypath = n.keypath;
        this.certpath = n.certpath;
        this.capath = n.capath;

        if (n.birthTopic) {
            this.birthMessage = {
                topic: n.birthTopic,
                payload: n.birthPayload || "",
                qos: Number(n.birthQos||0),
            };
        }

        // If the config node is missing certain options (it was probably deployed prior to an update to the node code),
        // select/generate sensible options for the new fields
        if (typeof this.keepalive === 'undefined'){
            this.keepalive = 60;
        } else if (typeof this.keepalive === 'string') {
            this.keepalive = Number(this.keepalive);
        }


        // Build options for passing to the aws sdk
        this.options.clientId = this.clientid || 'mqtt_' + (1+Math.random()*4294967295).toString(16);
        this.options.keepalive = this.keepalive;
        this.options.clean = true;  //AWS does not support non clean sessions

        if (n.willTopic) {
            this.options.will = {
                topic: n.willTopic,
                payload: n.willPayload || "",
                qos: Number(n.willQos||0),
                retain: false
            };
        }

        this.options.keyPath = this.keypath;
        this.options.certPath = this.certpath;
        this.options.caPath = this.capath;
        this.options.port =  this.port;
        this.options.host = this.broker;
        // Define functions called by MQTT in and out nodes
        var node = this;
        this.users = {};

        this.register = function(mqttNode){
            node.users[mqttNode.id] = mqttNode;
            if (Object.keys(node.users).length === 1) {
                node.connect();
            }
        };

        this.deregister = function(mqttNode,done){
            delete node.users[mqttNode.id];
            if (node.closing) {
                return done();
            }
            if (Object.keys(node.users).length === 0) {
                if (node.client) {
                    return node.client.end(done);
                }
            }
            done();
        };

        this.connect = function () {
            if (!node.connected && !node.connecting) {
                node.connecting = true;
                node.client = awsIot.device(node.options);		
                node.client.setMaxListeners(0);
                // Register successful connect or reconnect handler
                node.client.on('connect', function () {
                    node.connecting = false;
                    node.connected = true;
                    node.log(RED._("aws-iot.state.connected",{broker:(node.options.clientId?node.options.clientId+"@":"")+node.broker}));
                    for (var id in node.users) {
                        if (node.users.hasOwnProperty(id)) {
                            node.users[id].status({fill:"green",shape:"dot",text:"common.status.connected"});
                        }
                    }
                    // Remove any existing listeners before resubscribing to avoid duplicates in the event of a re-connection
                    node.client.removeAllListeners('message');

                    // Re-subscribe to stored topics
                    for (var s in node.subscriptions) {
                        var topic = s;
                        var qos = 0;
                        for (var r in node.subscriptions[s]) {
                            qos = Math.max(qos,node.subscriptions[s][r].qos);
                            node.client.on('message',node.subscriptions[s][r].handler);
                        }
                        var options = {qos: qos};
                        node.client.subscribe(topic, options);
                    }

                    // Send any birth message
                    if (node.birthMessage) {
                        node.publish(node.birthMessage);
                    }
                });
                node.client.on("reconnect", function() {
                    for (var id in node.users) {
                        if (node.users.hasOwnProperty(id)) {
                            node.users[id].status({fill:"yellow",shape:"ring",text:"common.status.connecting"});
                        }
                    }
                })
                // Register disconnect handlers
                node.client.on('close', function () {
                    if (node.connected) {
                        node.connected = false;
                        node.log(RED._("aws-iot.state.disconnected",{broker:(node.options.clientId?node.options.clientId+"@":"")+node.broker}));
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                node.users[id].status({fill:"red",shape:"ring",text:"common.status.disconnected"});
                            }
                        }
                    } else if (node.connecting) {
                        node.log(RED._("aws-iot.state.connect-failed",{broker:(node.options.clientId?node.options.clientId+"@":"")+node.broker}));
                    }
                });

                // Register connect error handler
                node.client.on('error', function (error) {
                    if (node.connecting) {
                        node.client.end();
                        node.connecting = false;
                    }
                });
            }
        };

        this.subscribe = function (topic,qos,callback,ref) {
            ref = ref||0;
            node.subscriptions[topic] = node.subscriptions[topic]||{};
            var sub = {
                topic:topic,
                qos:qos,
                handler:function(mtopic,mpayload, mpacket) {
                    if (matchTopic(topic,mtopic)) {
                        callback(mtopic,mpayload, mpacket);
                    }
                },
                ref: ref
            };
            node.subscriptions[topic][ref] = sub;
            if (node.connected) {
                node.client.on('message',sub.handler);
                var options = {};
                options.qos = qos;
                node.client.subscribe(topic, options);
            }
        };

        this.unsubscribe = function (topic, ref) {
            ref = ref||0;
            var sub = node.subscriptions[topic];
            if (sub) {
                if (sub[ref]) {
                    node.client.removeListener('message',sub[ref].handler);
                    delete sub[ref];
                }
                if (Object.keys(sub).length == 0) {
                    delete node.subscriptions[topic];
                    if (node.connected){
                        node.client.unsubscribe(topic);
                    }
                }
            }
        };

        this.publish = function (msg) {
            if (node.connected) {
                if (!Buffer.isBuffer(msg.payload)) {
                    if (typeof msg.payload === "object") {
                        msg.payload = JSON.stringify(msg.payload);
                    } else if (typeof msg.payload !== "string") {
                        msg.payload = "" + msg.payload;
                    }
                }

                var options = {
                    qos: msg.qos || 0,
                    retain: false
                };
                node.client.publish(msg.topic, msg.payload, options, function (err){return});
            }
        };

        this.on('close', function(done) {
            this.closing = true;
            if (this.connected) {
                this.client.once('close', function() {
                    done();
                });
                this.client.end();
            } else {
                done();
            }
        });

    }
    RED.nodes.registerType("aws-iot-broker",AWS_IOTBrokerNode);

    function AWS_IOTInNode(n) {
        RED.nodes.createNode(this,n);
        this.topic = n.topic;
        this.broker = n.broker;
        this.brokerConn = RED.nodes.getNode(this.broker);
        if (!/^(#$|(\+|[^+#]*)(\/(\+|[^+#]*))*(\/(\+|#|[^+#]*))?$)/.test(this.topic)) {
            return this.warn(RED._("aws-iot.errors.invalid-topic"));
        }
        var node = this;
        if (this.brokerConn) {
            this.status({fill:"red",shape:"ring",text:"common.status.disconnected"});
            if (this.topic) {
                node.brokerConn.register(this);
                this.brokerConn.subscribe(this.topic,1,function(topic,payload,packet) {
                    if (isUtf8(payload)) { payload = payload.toString(); }
                    var msg = {topic:topic,payload:payload, qos: packet.qos};
                    if ((node.brokerConn.broker === "localhost")||(node.brokerConn.broker === "127.0.0.1")) {
                        msg._topic = topic;
                    }
                    node.send(msg);
                }, this.id);
                if (this.brokerConn.connected) {
                    node.status({fill:"green",shape:"dot",text:"common.status.connected"});
                }
            }
            else {
                this.error(RED._("aws-iot.errors.not-defined"));
            }
            this.on('close', function(done) {
                if (node.brokerConn) {
                    node.brokerConn.unsubscribe(node.topic,node.id);
                    node.brokerConn.deregister(node,done);
                }
            });
        } else {
            this.error(RED._("aws-iot.errors.missing-config"));
        }
    }
    RED.nodes.registerType("aws-iot-in",AWS_IOTInNode);

    function AWS_IOTOutNode(n) {
        RED.nodes.createNode(this,n);
        this.topic = n.topic;
        this.qos = n.qos || null;
        this.broker = n.broker;
        this.brokerConn = RED.nodes.getNode(this.broker);
        var node = this;

        if (this.brokerConn) {
            this.status({fill:"red",shape:"ring",text:"common.status.disconnected"});
            this.on("input",function(msg) {
                if (msg.qos) {
                    msg.qos = parseInt(msg.qos);
                    if ((msg.qos !== 0) && (msg.qos !== 1) && (msg.qos !== 2)) {
                        msg.qos = null;
                    }
                }
                msg.qos = Number(node.qos || msg.qos || 0);
                msg.retain = false;
                if (node.topic) {
                    msg.topic = node.topic;
                }
                if ( msg.hasOwnProperty("payload")) {
                    if (msg.hasOwnProperty("topic") && (typeof msg.topic === "string") && (msg.topic !== "")) { // topic must exist
                        this.brokerConn.publish(msg);  // send the message
                    }
                    else { node.warn(RED._("aws-iot.errors.invalid-topic")); }
                }
            });
            if (this.brokerConn.connected) {
                node.status({fill:"green",shape:"dot",text:"common.status.connected"});
            }
            node.brokerConn.register(node);
            this.on('close', function(done) {
                node.brokerConn.deregister(node,done);
            });
        } else {
            this.error(RED._("aws-iot.errors.missing-config"));
        }
    }
    RED.nodes.registerType("aws-iot-out",AWS_IOTOutNode);
};
