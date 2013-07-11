var neo = require('neo4j')
  , async = require('async')
  , fs = require('fs')
  , extend = require('xtend')
  , db;
  
var opts = {
	db_string: "http://localhost:7474",
	proto_dir: __dirname + "/models"
};
  
var core = {
	create: function(proto, obj, callback) {
		proto.required_fields = proto.required_fields || proto.reqs || {};
		this.validate(obj, proto.required_fields, function(err){
			if (err) callback(err);
			var node = db.createNode(obj);
			node.save(function(err, n){
				this.spawn(n, proto, callback);
			});
		});
	},
	
	createFromId: function(proto, id, callback) {
		db.getNodeById(id, function(err, n){
			this.spawn(n, proto, callback);
		});
	},
	
	createFromIndex: function(proto, index, key, value, callback) {
		db.getIndexedNodes(index, key, value, function(err, results){
			this.spawn(results[0], proto, callback);
		});
	},
	
	createSet: function(proto, objs, options, callback) {
		if (typeof (options) === 'function') callback = options, options = {};
		var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
		var method = parallel ? 'each' : 'eachSeries';
		var results = [];
		async[method](objs, function(item, f){
			this.create(proto, item, function(err, obj) {
				results.push(obj);
				f();
			});
		}, function(err){
			this.parseSet(options, results, callback);
		});
	},
	
	createSetFromIds: function(proto, ids, options, callback) {
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
						this.createFromId(proto, i, function(err, obj){
							results.push(obj);
							f();
						});
					}, function(err){
						this.parseSet(options, results, callback);
					});
				}
			);
		}
	},
	
	createSetFromIndex: function(proto, index, key, value, options, callback) {
		if (typeof options === 'function') callback = options, options = {};
		var parallel = typeof(options.parallel) === 'undefined' ? true : options.parallel;
		var method = parallel ? 'each' : 'eachSeries';
		var results = [];
		db.getIndexedNodes(index, key, value, function(err, results){
			async[method](results, function(item, f){
				this.spawn(item, proto, function(err, obj){
					results.push(obj);
					f();
				});
			}, function(err){
				this.parseSet(options, results, callback);
			});
		});
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
	
	validate: function(obj, reqs, cb) {
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
	
	spawn: function(node, proto, cb) {
		var obj = Object.create(proto);
		async.each(Object.keys(node._data.data), function(item, f){
			obj[item] = node._data.data[item];
			obj.node = node;
			f();
		}, function(err){
			cb(err, obj);
		});
	},
	
	load: function(options) {
		opts = extend(opts, options);
		db = new neo.GraphDatabase(opts.db_string);
		fs.readdir(opts.proto_dir, function(err, files){
			
		});
	}
}
