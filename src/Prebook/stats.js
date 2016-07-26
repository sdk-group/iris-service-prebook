'use strict'
let discover = require('./stat-method/index.js');

class Gatherer {
	constructor() {
		this._initialized = false;
		this._locked = {
			value: false,
			section: []
		};
		this._dataset = {};
		this._consumers = {};
		this._computed = {};
		this.timestamp = {};
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

	setThrottle(thr) {
		this._throttle = thr * 1000;
	}

	_fill(data) {
		//parse data
		this._dataset = _.cloneDeep(data);
		//set flag
		this._initialized = true;
	}

	lock(section) {
		this._locked.value = true;
	}

	unlock(section) {
		this._locked.value = false;
	}

	get locked() {
		return this._locked.value;
	}

	alive(section) {
		console.log(this.timestamp);
		return (_.get(this.timestamp, section, _.get(this.timestamp, '_last', 0)) + this._ttl > _.now());
	}

	recent(section) {
		return (_.get(this.timestamp, section, _.get(this.timestamp, '_last', 0)) + this._throttle > _.now());
	}

	invalidate(section) {
		console.log("INVALIDATE", section, this.recent(section));
		if (!this.recent(section))
			_.set(this.timestamp, section, 0);
	}

	get ready() {
		return this._initialized;
	}

	stats(section) {
		if (!this.ready)
			throw new Error("Ain't ready to give you stats");
		let cmp = _.get(this._computed, section, false);
		if (cmp)
			return cmp;
		let dataset = section ? _.get(this._dataset, section, false) : this._dataset;
		let res = _.mapValues(dataset, (datapart) => {
			return _.mapValues(this._consumers, (fn) => fn(datapart));
		});
		_.set(this._computed, section, res);
		return Promise.resolve(res);
	}

	update(data, section = false) {
		if (!section) {
			this._fill(data);
		} else {
			_.set(this.timestamp, section, _.now());
			console.log("set ts", this.timestamp);
			_.set(this._dataset, section, data);
		}
		_.set(this.timestamp, ['_last'], _.now());
		_.unset(this, '_computed');
	}

	flush() {
		this._initialized = false;
		_.unset(this, '_dataset');
		_.unset(this, '_computed');
	}

}

let instance = new Gatherer();
module.exports = instance;