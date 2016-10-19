'use strict'
let discover = require('./stat-method/index.js');

class Gatherer {
	constructor() {
		this._locked = {};
		this._dataset = {};
		this._consumers = {};
		this._computed = {};
		this.timestamp = {};
		this._expiry = {};
	}

	setTransforms(names) {
		_.map(_.castArray(names), nm => {
			let kebab = _.kebabCase(nm);
			let pack = discover(kebab);
			this._consumers[pack.name] = pack.compute;
		});
	}

	lockSections(sections = '_global') {
		_.map(_.castArray(sections), sc => this.lock(sc));
	}

	unlockSections(sections = '_global') {
		_.map(_.castArray(sections), sc => this.unlock(sc));
	}

	unlock(section = '_global') {
		// console.log("unlock", section, this._locked);
		this._locked[section] = {
			value: false
		};
		// console.log("unlockres", section, this._locked);
	}

	lock(section = '_global') {
		// console.log("lock", section, this.locked(section));
		if (this.locked(section))
			return Promise.reject(new Error(`Section ${section} is locked.`));
		// console.log("lock", section, this._locked);
		this._locked[section] = {
			value: true,
			ts: _.now()
		};
		// console.log("lockres", section, this._locked);
	}

	locked(section) {
		return this._locked && this._locked._global && this._locked._global.value || false || this._locked && this._locked[section] && this._locked[section].value || false;
	}

	invalidate(section) {
		this._expiry[section] = 0;
	}

	expired(section) {
		return (this._expiry[section] || 0) <= _.now();
	}

	stats(section) {
		let cmp = this._computed[section] || false;
		if (cmp)
			return cmp;
		let dataset = this._dataset[section] || {};
		let res = _.mapValues(dataset, (datapart) => {
			return _.mapValues(this._consumers, (fn) => fn(datapart));
		});
		this._computed[section] = res;
		return Promise.resolve(res);
	}

	update(section, data) {
		this._computed && (this._computed[section] = null);
		this.timestamp[section] = _.now();
		// console.log("set ts", this.timestamp);
		this._dataset[section] = _.cloneDeep(data);
	}

	setExpiry(section, ts) {
		this._expiry[section] = ts;
	}

}

let instance = new Gatherer();
module.exports = instance;