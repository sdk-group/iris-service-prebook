'use strict'
let discover = require('./stat-method/index.js');

class SlotStats {
	constructor() {
		this._initialized = false;
		this._dataset = {};
		this._consumers = {};
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

	get alive() {
		return (this.timestamp + this._ttl > _.now());
	}

	get ready() {
		return this._initialized;
	}

	stats(path) {
		if (!this.ready)
			throw new Error("Ain't ready to give you stats");
		let dataset = path ? _.get(this._dataset, path, false) : this._dataset;
		return _.mapValues(dataset, (datapart) => {
			return _.mapValues(this._consumers, (fn) => fn(datapart));
		});
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
	}

}

let instance = new SlotStats();
module.exports = instance;