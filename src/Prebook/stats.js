'use strict'
let discover = require('./stat-method/index.js');

class Gatherer {
	constructor() {
		this._initialized = false;
		this._locked = false;
		this._dataset = {};
		this._consumers = {};
		this._computed = {};
	}

	setTransforms(names) {
		_.map(_.castArray(names), nm => {
			let kebab = _.kebabCase(nm);
			let pack = discover(kebab);
			this._consumers[pack.name] = pack.compute;
		});
	}

	setTtl(ttl) {
		this._ttl = ttl * 1000;
	}

	_fill(data) {
		//parse data
		this._dataset = _.cloneDeep(data);
		//set flag
		this._initialized = true;
	}

	lock() {
		this._locked = true;
	}

	unlock() {
		this._locked = false;
	}

	get locked() {
		return this._locked;
	}

	get alive() {
		return (this.timestamp + this._ttl > _.now());
	}

	get ready() {
		return this._initialized;
	}

	stats(path) {
		if (!this.ready)
			throw new Error("Ain't ready to give you stats");
		let cmp = _.get(this._computed, path, false);
		if (cmp)
			return cmp;
		let dataset = path ? _.get(this._dataset, path, false) : this._dataset;
		let res = _.mapValues(dataset, (datapart) => {
			return _.mapValues(this._consumers, (fn) => fn(datapart));
		});
		_.set(this._computed, path, res);
		return res;
	}

	update(data, path = false) {
		this.timestamp = _.now();
		if (!path) {
			this._fill(data);
		} else {
			this._dataset = _.merge(this._dataset, data);
		}
	}

	flush() {
		this._initialized = false;
		_.unset(this, '_dataset');
		_.unset(this, '_computed');
	}

}

let instance = new Gatherer();
module.exports = instance;