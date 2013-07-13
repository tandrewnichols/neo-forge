var neo = require('neo4j')
  , async = require('async')
  , fs = require('fs')
  , EventEmitter = require('events').EventEmitter
  , extend = require('xtend')
  , filemap = {}
  , db;
  
var defaults = {
	dbString: "http://localhost:7474",
	protoDir: __dirname + "/models",
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
	db = new neo.GraphDatabase(opts.dbString);
	
	// define the node object as a getter so that 'this' works correctly
	self.__defineGetter__('node', function(){
		return {
			/*
			 * Pass in an object with any properties to save a node in neo
			 * and make that node into an object with a prototype of methods
			 */
			fromObj: function(req, obj, callback) {
				var reqs = req.required || req.required_fields || req.reqs || [];
				self.validate(obj, reqs, function(err){
					var node = db.createNode(obj);
					node.save(function(err, n){
						self.spawn(n, req, callback);
					});
				});
			},
			
			/*
			 * Pass in an id to get an existing node from neo and construct a
			 * prototypal object with it
			 */
			fromId: function(req, id, callback) {
				db.getNodeById(id, function(err, n){
					self.spawn(n, req, callback);
				});
			},
			
			/*
			 * Get a prototypal object from a neo index
			 */
			fromIndex: function(req, index, key, value, callback) {
				db.getIndexedNodes(index, key, value, function(err, results){
					self.spawn(results[0], req, callback);
				});
			},
			
			/*
			 * Methods pertaining to sets of objects
			 */
			set: {
				
				/*
				 * Corresponds to fromObj, but for sets of objects
				 */
				fromObjs: function(req, objs, options, callback) {
					if (typeof (options) === 'function') callback = options, options = {};
					var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
					var method = parallel ? 'each' : 'eachSeries';
					var results = [];
					async[method](objs, function(item, f){
						self.node.fromObj(req, item, function(err, obj) {
							results.push(obj);
							f();
						});
					}, function(err){
						self.parseSet(options, results, callback);
					});
				},
				
				/*
				 * Corresponds to fromId, but for sets of objects
				 */
				fromIds: function(req, ids, options, callback) {
					if (typeof (options) === 'function') callback = options, options = {};
					var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
					var method = parallel ? 'each' : 'eachSeries';
					var results = [];
					if (typeof (ids) === 'string') {
						var ends = ids.split('-');
						ids = [];
						async.whilst(
							function(){ return ends[0] <= ends[1]; },
							function(callback) {
								ids.push(ends[0]++);
								callback();
							},
							function(err){
								async[method](ids, function(i, f){
									self.node.fromId(req, i, function(err, obj){
										results.push(obj);
										f();
									});
								}, function(err){
									self.parseSet(options, results, callback);
								});
							}
						);
					}
				},
				
				/*
				 * Still takes only one index, key, and value, but returns all matching
				 * results, rather than just the first. Prefer this if the results of
				 * an indexed search are not guaranteed to be unique.
				 */
				fromIndex: function(req, index, key, value, options, callback) {
					if (typeof options === 'function') callback = options, options = {};
					var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
					var method = parallel ? 'each' : 'eachSeries';
					var results = [];
					db.getIndexedNodes(index, key, value, function(err, results){
						async[method](results, function(item, f){
							self.spawn(item, req, function(err, obj){
								results.push(obj);
								f();
							});
						}, function(err){
							self.parseSet(options, results, callback);
						});
					});
				},
				
				/*
				 * Create multiple objects with the same values. Useful if objects tend
				 * to have several values in common. You can then add unique properties later.
				 */
				fromTemplate: function(req, obj, count, callback) {
					var reqs = req.required || req.required_fields || req.reqs || [];
					self.validate(obj, reqs, function(err){
						if (err) callback(err);
						async.times(count, function(i, next){
							self.node.fromObj(req, obj, next);
						}, callback);
					});
				},
				
				/*
				 * Gets a set of nodes that matches any passed in query.
				 * You can pass in either cypher or a lucene index query.
				 */
				fromQuery: function(req, cypher, vars, callback) {
					var results = [];
					var handle_results = function(err, nodes){
						async.each(nodes, function(item, f){
							self.spawn(item, req, function(err, obj){
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
		
		// get all files in currect directory
		fs.readdir(root, function(err, files){
			
			// and for each, in tandem
			async.each(files, function(file, f){
				
				// get info
				fs.stat(file, function(err, stat){
					
					// recurse if it's another directory
					if (opts.recursive && stat.isDirectory()) {
						read(root + '/' + file, cb);
					} else {
						
						if (opts.filter.indexOf(file) !== -1) {
							// or require the module, add it to filemap so we can
							// reference it later, and build the custom methods.
							var req = require(root + '/' + file);
							
							// TODO: Add namespacing here
							filemap[file] = req;
							self.buildMethods(req, file, function(methods){
								self[file] = methods;
								f();
							});
						} else f();
					}
				});
			}, cb);
		});
	};
	
	// initial call to either the passed in directory or the default
	read(opts.protoDir, function(err){
		self.emit('loaded');
	});
};

/*
 * Inherit from event emitter
 */
Forge.prototype.__proto__ = EventEmitter.prototype;

/*
 * Create an object from a prototype and add a variety of properties
 * and additional functions
 */
Forge.prototype.spawn = function(node, req, cb) {
	var self = this;
	
	// create the object
	var obj = Object.create(req);
	
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
		
		// Build navigation properties and some helper instance methods
		// in parallel, since they have no depencies on one another
		async.parallel([
			function(callback){
				self.buildNavigation(obj, req, function(err, obj){
					callback(err);
				});
			},
			function(callback){
				self.buildInstance(obj, function(err, obj){
					callback(err);
				});
			}
		], function(err, results){
			cb(err, obj);
		});
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
Forge.prototype.buildMethods = function(req, file, cb) {
	var self = this;
	var obj = {
		fromObj: function(obj, callback) { self.node.fromObj(req, arguments) },
		fromId: function(id, callback) { self.node.fromId(req, arguments) },
		fromIndex: function(index, key, value, callback) { self.node.fromIndex(req, arguments) },
		fromNode: function(node, callback) { self.spawn(req, arguments) },
		set: {
			fromObjs: function(objs, options, callback) { self.node.set.fromObjs(req, arguments) },
			fromIds: function(ids, options, callback) { self.node.set.fromIds(req, arguments) },
			fromIndex: function(index, key, value, options, callback) { self.node.set.fromIndex(req, arguments) },
			fromTemplate: function(obj, count, callback) { self.node.set.fromTemplate(req, arguments) },
			fromQuery: function(cypher, vars, callback) { self.node.set.fromQuery(req, arguments) }
		}
	};
	cb(obj);
}

/*
 * Sets up navigation properties for each model, based on paths defined in the module
 */
Forge.prototype.buildNavigation = function(obj, req, cb) {
	var self = this;
	
	// get navigation map
	var navs = req.navigation || req.nav || req.navigation_properties || {};
	var results = [];
	
	// loop over the keys
	async.each(Object.keys(navs), function(nav, f){
		
		// and create a function on the object that returns node endpoints
		obj[nav] = function(opts, callback){
			if (typeof opts === 'function') {
				callback = opts;
				opts = {};
			}
			
			var items = navs[nav].split(' ');
			
			// get the final node name so we can reference it later
			var endpoint = items[items.length - 1];
			var match = "MATCH st";
			
			// simple placeholder variables - shouldn't need more than 26
			var names = "abcdefghijklmnopqrstuvwxyz".split("");
			var used = [];
			
			// loop over each nav piece, needs to be done in series since the order matters
			async.eachSeries(items, function(item, g){
				
				// if it's all caps, treat it as a relationship
				if (item.match(/[A-Z_]+/)) {
					
					// get a temporary variable for this piece and put in used
					var rel = names.shift();
					used.push(rel);
					
					// construct a cypher relationsihp match in the form
					// -[r:VERB]-> where r is the placeholder and VERB
					// is the relationship type
					match += '-[' + rel + ':' + item + ']->';
					
				} else {
					var node = names.shift();
					used.push(node);
					match += node;
				}
				g();
			}, function(err){
				
				// construct the cypher
				var cypher = [
					"START st=node({id})",
					match
				];
				if (opts.where) cypher.push('WHERE ' + opts.where);
				cypher.push('RETURN ' + end);
				var query = cypher.join("\n");
				
				// get the last placeholder so we can return it
				var end = used.pop();
				
				// query the graph
				db.query(query, {id: obj.id}, function(err, matches) {
					
					// loop over the matches 
					async.each(matches, function(match, g){
						
						// if this is a type that we have a proto for,
						// make an object out of it and add it to the results
						if (filemap[endpoint]) {
							self.spawn(filemap[endpoint], match[end], function(err, obj){
								results.push(obj);
							});
						} else {
							
							// otherwise, push the node itself
							results.push(match[end]);
						}
					}, function(err){
						
						// finally, return the results
						// this let's you make an object, then call
						// object.nodesOfSomeType() and get nodes
						// connected to this one
						callback(err, results);
					});
				});
			});
		}
	}, function(err){
		cb(err, obj);
	});
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
			var i = JSON.stringify(obj[item]);
			var n = obj.node._data.data[item];
			
			// if it's changed and it's not a function
			if (i && typeof i !== 'function' && i !== n) {
				
				// if it's an object, first stringify it since neo
				// can't store more than simple types
				if (typeof i === 'object') {
					obj.node._data.data[item] = i;
				} else {
					obj.node._data.data[item] = i;
				}
			} else if (!obj[item]) {
				// if the property has been removed, remove the corresponding
				// node value
				delete obj.node._data.data[item];
			}
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
	
	// Maps to either node.createRelationshipTo or node.createRelationshipFrom depending
	// on the value of dir.
	obj.createRelationship = function(dir, otherNode, type, data, f) {
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
}



/*
 * TODO
 * 1. caching
 * 2. namespacing
 */