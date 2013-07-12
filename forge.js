var neo = require('neo4j')
  , async = require('async')
  , fs = require('fs')
  , extend = require('xtend')
  , db;
  
var defaults = {
	db_string: "http://localhost:7474",
	proto_dir: __dirname + "/models",
	recursive: true,
	namespace: true,
	filter: []
};
  
var forge = {
	node: {
		fromObj: function(req, obj, callback) {
			var reqs = req.required || req.required_fields || req.reqs || [];
			validate(obj, reqs, function(err){
				var node = db.createNode(obj);
				node.save(function(err, n){
					forge.spawn(n, req, callback);
				});
			}
		},
		fromId: function(req, id, callback) {
			db.getNodeById(id, function(err, n){
				forge.spawn(n, req, callback);
			});
		},
		fromIndex: function(req, index, key, value, callback) {
			db.getIndexedNodes(index, key, value, function(err, results){
				forge.spawn(results[0], req, callback);
			});
		},
		set: {
			fromObjs: function(req, objs, options, callback) {
				if (typeof (options) === 'function') callback = options, options = {};
				var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
				var method = parallel ? 'each' : 'eachSeries';
				var results = [];
				async[method](objs, function(item, f){
					forge.node.fromObj(req, item, function(err, obj) {
						results.push(obj);
						f();
					});
				}, function(err){
					forge.parseSet(options, results, callback);
				});
			},
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
								forge.node.fromId(req, i, function(err, obj){
									results.push(obj);
									f();
								});
							}, function(err){
								forge.parseSet(options, results, callback);
							});
						}
					);
				}
			},
			fromIndex: function(req, index, key, value, options, callback) {
				if (typeof options === 'function') callback = options, options = {};
				var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
				var method = parallel ? 'each' : 'eachSeries';
				var results = [];
				db.getIndexedNodes(index, key, value, function(err, results){
					async[method](results, function(item, f){
						forge.spawn(item, req, function(err, obj){
							results.push(obj);
							f();
						});
					}, function(err){
						forge.parseSet(options, results, callback);
					});
				});
			},
			fromTemplate: function(req, obj, count, callback) {
				var reqs = req.required || req.required_fields || req.reqs || [];
				validate(obj, reqs, function(err){
					if (err) callback(err);
					async.times(count, function(i, next){
						forge.node.fromObj(req, obj, next);
					}, callback);
				});
			},
			fromQuery: function(req, cypher, vars, callback) {
				var results = [];
				var handle_results = function(err, nodes){
					async.each(nodes, function(item, f){
						forge.spawn(item, req, function(err, obj){
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
	},  
	
	parseSet: function(options, set, callback) {
		if ('limit' in options && ('skip' in options || 'take' in options)) {
			delete options.skip;
			delete options.take;
		}
		async.waterfall([
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
			function(sorted, cb) {
				if ('limit' in options) {
					cb(sorted.splice(0, options.limit));
				} else cb(sorted);
			},
			function(limited, cb) {
				if ('take' in options) {
					cb(limited.slice(options.skip || 0, options.take));
				} else if ('skip' in options) {
					cb(limited.slice(options.skip));
				} else {
					cb(limited);
				}
			},
			function(subset, cb) {
				if ('where' in options) {
					async.filter(subset, options.where, cb);
				} else cb(subset);
			}
		], callback);
	},
	
	spawn: function(node, req, cb) {
		var obj = Object.create(req);
		async.each(Object.keys(node._data.data), function(item, f){
			obj[item] = node._data.data[item];
			f();
		}, function(err){
			obj.node = node;
			obj.id = node.id;
			forge.navigation(obj, req, cb);
		});
	},
	
	navigation: function (obj, req, cb) {
		var navs = req.navigation || req.nav || req.navigation_properties || {};
		async.each(Object.keys(navs), function(nav, f){
			obj[nav] = function(){
				var nodes = navs[nav].split('->');
				var match = 'MATCH ';
				var rels = "abcdefghijklmnopqrstuvwxyz".split("");
				var used = [];
				async.eachSeries(nodes, function(node, g){
					if (node.match(/[A-Z]+/)) {
						var rel = rels.shift();
						used.push(rel);
						match += '-[' + rel + ':' + node + ']->';
					} else {
						if (match === 'MATCH ') {
							match += 'nd';
						} else {
							var n = rels.shift();
							used.push(n);
							match += n;
						}
					}
					g();
				}, function(err){
					var cypher = [
						"START nd=node({id})",
						match,
						"RETURN " + used.join(", ");
					].join("\n");
					db.query(cypher, obj.id, function(err, results
				});
			}
		}, function(err){
			cb(err, obj);
		});
	},
	
	load: function(options) {
		var opts = extend(defaults, options);
		db = new neo.GraphDatabase(opts.db_string);
		var read = function(root, cb) {
			fs.readdir(root, function(err, files){
				async.each(files, function(file, f){
					fs.stat(file, function(err, stat){
						if (opts.recursive && stat.isDirectory()) {
							read(root + '/' + file, cb);
						} else {
							var req = require(root + '/' + file);
							buildMethods(req, file, function(methods){
								forge[file] = methods;
								f();
							});
						}
					});
				}, cb);
			});
		}
		
		read(opts.proto_dir, function(){
			
		});
	}
}

function validate (obj, reqs, cb) {
	async.each(reqs, function(item, f){
		if (!(item in obj) || obj[item] === undefined || obj[item] === ''){
			ok = 0;
		}
		f();
	}, function(err){
		if (err || !ok) cb(err || 'Required: ' + reqs.join(' '));
		else cb();
	});
},

function buildMethods (req, file, cb) {
	var obj = {
		fromObj: function(obj, callback) { forge.node.fromObj(req, arguments); },
		fromId: function(id, callback) { forge.node.fromId(req, arguments); },
		fromIndex: function(index, key, value, callback) { forge.node.fromIndex(req, arguments); },
		set: {
			fromObjs: function(objs, options, callback) { forge.node.set.fromObjs(req, arguments); },
			fromIds: function(ids, options, callback) { forge.node.set.fromIds(req, arguments); },
			fromIndex: function(index, key, value, options, callback) { forge.node.set.fromIndex(req, arguments); },
			fromTemplate: function(obj, count, callback) { forge.node.set.fromTemplate(req, arguments); },
			fromQuery: function(cypher, vars, callback) { forge.node.set.fromQuery(req, arguments); }
		}
	};
	cb(obj);
}

/*
 * TODO
 * 1. caching
 * 2. namespacing
 * 3. top level node.from
 * 4. Look for index in each dir first and require that
 * 5. Navigation properties
 * 6. Make event emitter that emits 'loaded'
 */