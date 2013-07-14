var neo = require('neo4j')
  , async = require('async')
  , fs = require('fs')
  , EventEmitter = require('events').EventEmitter
  , extend = require('xtend');
  
var defaults = {
	dbString: "http://localhost:7474",
	recursive: true,
	namespace: false,
	filter: []
};
 
/*
 * Load all model files into forge object
 */	
var Forge = module.exports = function Forge(options) {
	var self = this;
	var opts = extend(defaults, options);
	self.db = new neo.GraphDatabase(opts.dbString);
	
	// define the node object as a getter so that 'this' works correctly
	self.__defineGetter__('node', function(){
		return {		
			fromObj: function(name, obj, callback) {
				self.validate(obj, self.map[name].schema.nodes[name].required, function(err){
					var node = db.createNode(obj);
					node.save(function(err, n){
						async.each(self.map[name].schema.nodes[name].indexed, function(item, f){
							node.index(name, item, node._data.data[item], f)
						}, function(err){
							self.spawn(name, n, callback);
						})
					});
				});
			},
			
			fromId: function(name, id, callback) {
				db.getNodeById(id, function(err, n){
					self.spawn(name, n, callback);
				});
			},
			
			fromIndex: function(name, index, key, value, callback) {
				db.getIndexedNodes(index, key, value, function(err, results){
					self.spawn(name, results[0], callback);
				});
			},
			
			set: {
				fromObjs: function(name, objs, options, callback) {
					if (typeof (options) === 'function') callback = options, options = {};
					var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
					var method = parallel ? 'each' : 'eachSeries';
					var results = [];
					async[method](objs, function(item, f){
						self.node.fromObj(name, item, function(err, obj) {
							results.push(obj);
							f();
						});
					}, function(err){
						self.parseSet(options, results, callback);
					});
				},
				
				fromIds: function(name, ids, options, callback) {
					if (typeof (options) === 'function') callback = options, options = {};
					var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
					var method = parallel ? 'each' : 'eachSeries';
					var results = [];
					var process = function(err) {
						async[method](ids, function(i, f){
							self.node.fromId(name, i, function(err, obj){
								results.push(obj);
								f();
							});
						}, function(err){
							self.parseSet(options, results, callback);
						});
					}
					
					if (typeof (ids) === 'string') {
						var ends = ids.split('-');
						ids = [];
						async.whilst(
							function(){ return ends[0] <= ends[1]; },
							function(callback) {
								ids.push(ends[0]++);
								callback();
							},
							process
						);
					} else {
						process(null);
					}
				},
				
				fromIndex: function(name, index, key, value, options, callback) {
					if (typeof options === 'function') callback = options, options = {};
					var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
					var method = parallel ? 'each' : 'eachSeries';
					var results = [];
					db.getIndexedNodes(index, key, value, function(err, results){
						async[method](results, function(item, f){
							self.spawn(name, item, function(err, obj){
								results.push(obj);
								f();
							});
						}, function(err){
							self.parseSet(options, results, callback);
						});
					});
				},
				
				fromTemplate: function(name, obj, count, callback) {
					var reqs = req.required || req.required_fields || req.reqs || [];
					self.validate(obj, self.map[name].schema.nodes[name].required, function(err){
						if (err) callback(err);
						async.times(count, function(i, next){
							self.node.fromObj(name, obj, next);
						}, callback);
					});
				},
				
				fromQuery: function(req, cypher, vars, callback) {
					var results = [];
					var handle_results = function(err, nodes){
						async.each(nodes, function(item, f){
							self.spawn(name, item, function(err, obj){
								results.push(item);
								f();
							}, function(err){
								callback(err, results);
							});
						});
					};
					
					if (typeof vars === 'object') db.query(cypher, vars, handle_results);
					else if (typeof vars === 'string') db.queryNodeIndex(vars, cypher, handle_results);
				}
			}
		}
	});
	
	// recursive method to load a directory
	var read = function(root, cb) {		
		async.each(Object.keys(self.schema.nodes), function(file, f){
			fs.exists(root + '/' + file, function(err, exists){
				if (exists) {
					self.map[file] = {
						proto: require(root + '/' + file),
						schema: self.schema.nodes[file]
					}
					self.buildMethods(req, file, function(methods){
						self[file] = methods;
						f();
					});
				} else {
					// TODO: Add namespace checking?
					f();
				}
			});
		});
	};
	
	if(opts.schemaDir) {
		self.schema = require(opts.schemaDir + '/schema.json');
		read(opts.schemaDir, function(err){
			self.emit('loaded');
		});
	} else {
		self.getSchemaDir(function(err, dir){
			read(dir, function(err){
				self.emit('loaded');
			});
		});
	}
};

/*
 * Inherit from event emitter
 */
Forge.prototype.__proto__ = EventEmitter.prototype;

/*
 * Create an object from a prototype and add a variety of properties
 * and additional functions
 */
Forge.prototype.spawn = function(name, node, cb) {
	var self = this;
	var proto = typeof name === object ? name : self.map[name].proto;
	// create the object
	var obj = Object.create(proto);
	
	// add the node properties as top level properties on the object
	// so that we don't have to do node._data.data every time
	async.each(Object.keys(node._data.data), function(item, f){
		if (node._data.data[item].match(/[\{\}\[\]]/)) obj[item] = JSON.parse(node._data.data[item]);
		else obj[item] = node._data.data[item];
		f();
	}, function(err){
		
		// Add the node itself, so we can call other methods on it
		// if necessary. Also add the id and db to the node. Adding
		// the db allows users to write their own "navigation" methods
		// using custom queries via this.db.query
		obj.node = node;
		obj.id = node.id;
		obj.db = db;
		obj.name = name;
		
		self.buildInstance(name, obj, cb);
	});
}
	
/*
 * Take a set and process a variety of options.
 * Possibly options are:
 * 		limit - [number] reduce the set of elements to n elements
 * 		skip - [number] omit the first n elements
 * 		take - [number] take n elements
 * 		sort - [string] property to sort on
 * 		where - [function] get only matching elements; should return a true or false (via callback)
 * 		paginate - [number] group into sub sets of n elements
 */
Forge.prototype.parseSet = function(options, set, callback) {
	var self = this;
	
	async.waterfall([
		// first, sort the elements
		function(cb) {
			if ('sort' in options) {
				async.sortBy(set, function(item, f){
					f(null, item[options.sort]);
				}, function(sorted) {
					if ('dir' in options && options.dir === 'desc') callback(sorted.reverse());
					else (sorted);
				});
			} else cb(set);
		},
		// then take and/or skip
		function(sorted, cb) {
			if ('take' in options) {
				cb(sorted.slice(options.skip || 0, options.take));
			} else if ('skip' in options) {
				cb(sorted.slice(options.skip));
			} else {
				cb(sorted);
			}
		},
		// then filter
		function(subset, cb) {
			if ('where' in options) {
				async.filter(subset, options.where, cb);
			} else cb(subset);
		},
		// then reduce the set the desired number of elements
		function(filtered, cb) {
			if ('limit' in options) {
				cb(filtered.splice(0, options.limit));
			} else cb(filtered);
		},
		// and finally, group into paginated subsets
		function(limited, cb) {
			if ('paginated' in options && limited.length > options.paginated) {
				var grouped = [];
				async.whilst(
					// while we still have items
					function() { limited.length },
					// segment them into groups
					function(callback) {
						// the last group: push all remaining items
						if (limited.length < options.paginated) {
							grouped.push(limited);
						} else {
							// for other groups, splice the first set off and push to new array
							// so that you end up with an array of arrays
							grouped.push(limited.splice(0, options.paginated));
						}
					},
					function(err){
						cb(err, grouped);
					}
				);
			} else { 
				cb(limited);
			}
		}
	], callback);
}

/*
 * Create accessor methods for each type
 */
Forge.prototype.buildMethods = function(file, cb) {
	var self = this;
	var obj = {
		fromObj: function(obj, callback) { self.node.fromObj(arguments) },
		fromId: function(id, callback) { self.node.fromId(arguments) },
		fromIndex: function(index, key, value, callback) { self.node.fromIndex(arguments) },
		fromNode: function(node, callback) { self.spawn(arguments) },
		set: {
			fromObjs: function(objs, options, callback) { self.node.set.fromObjs(arguments) },
			fromIds: function(ids, options, callback) { self.node.set.fromIds(arguments) },
			fromIndex: function(index, key, value, options, callback) { self.node.set.fromIndex(arguments) },
			fromTemplate: function(obj, count, callback) { self.node.set.fromTemplate(arguments) },
			fromQuery: function(cypher, vars, callback) { self.node.set.fromQuery(arguments) }
		}
	};
	cb(obj);
}

Forge.prototype.validate = function(obj, reqs, cb) {
	var self = this;
	async.each(reqs, function(item, f){
		if (!(item in obj) || obj[item] === undefined || obj[item] === ''){
			ok = 0;
		}
		f();
	}, function(err){
		if (err || !ok) cb(err || 'Required: ' + reqs.join(' '));
		else cb();
	});
}

Forge.prototype.buildInstance = function(obj, cb) {
	var self = this;
	
	// Simple proxy
	obj['delete'] = function(f) {
		obj.node['delete'](f);
	};
	
	// alias because delete is a reserved word
	obj.del = obj['delete'];
	
	// Sync up properties on the object and the node
	obj.save = function(f) {
		
		// loop over object keys
		async.each(Object.keys(obj), function(item, g){
			
			// stringify the object property for comparison
			var i = typeof obj[item] === 'object' ? JSON.stringify(obj[item]) : obj[item];
			var n = obj.node._data.data[item];
			
			// If it's changed and it's not a function, update it.
			// If it's gone, remove it
			if (i && typeof i !== 'function' && i !== n) obj.node._data.data[item] = i;
			else if (!i) delete obj.node._data.data[item];
			
		}, function(err){
			// finally, perpetuate these changes
			obj.node.save(f);
		});
	};
	
	// Adds a new property, or set of properties, to the 
	// object and its node
	obj.add = function(key, value, f) {
		var keyedObj = {};
		if (typeof value === 'function') {
			f = value, value = '';
			keyedObj = key;
		} else keyedObj[key] = value;
		
		async.each(Object.keys(keyedObj), function(item, g){
			obj[item] = keyedObj[item];
			obj.node._data.data[item] = keyedObj[item];
		}, f)
	};
	
	// Removes a property, or set of properties, from the object and its node
	obj.remove = function(props, f) {
		if (typeof props === 'string') {
			props = [props];
		}
		
		async.each(props, function(prop, g){
			delete obj[prop];
			delete obj.node._data.data[prop];
		}, f);
	};
	
	// Simple proxy
	obj.index = function(index, key, value, f) {
		obj.node.index(arguments);
	}
	
	// Add a relationship between two nodes
	obj.bind = function(dir, otherNode, type, data, f) {
		if (!dir.match(/(to|from)/)) f('Acceptable directions are "to" and "from."');
		var method = dir === 'to' ? 'createRelationshipTo' : 'createRelationshipFrom';
		
		if (typeof otherNode === 'number') {
			// If an id is passed in, look up the node
			db.getNodeById(otherNode, function(err, node){
				obj.node[method](node, type, data, f);
			});
		} else {
			// If a "forged" object is passed in, use the node property.
			// Otherwise, use the node passed in, as is.
			if (obj.hasOwnProperty('node')) {
				obj.node[method](otherNode.node, type, data, f);
			} else obj.node[method](otherNode, type, data, f);
		}
	}
	
	// Remove a relationship between two nodes
	obj.sever = function(otherNode, rel, f) {
		var other = typeof otherNode === 'number' ? otherNode : otherNode.id;
		if (typeof rel === 'function') f = rel, rel = '';
		var cypher = [
			"START n=node({id})",
			"MATCH n-[r:" + rel.toUpperCase() + "]-x",
			"WHERE x.id = {other}",
			"DELETE r"
		].join("\n");
		self.db.query(cypher, {id: obj.id, other: other}, f);
	}
	
	cb(null, obj);
}

Forge.prototype.getSchemaDir = function(cb) {
	var dirs = "schema models protos neo objects".split(" ");
	var dir;
	async.each(dirs, function(item, f){
		fs.exists(__dirname + '/' + item, function(err, exists){
			if (exists) dir = item;
		}, function(err){
			cb(err, dir);
		});
	});
}



/*
 * TODO
 * 1. caching
 * 2. namespacing
 * 3. navigation properties and other uses of schema.json
 */