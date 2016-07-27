'use strict'
let discover = require('./stat-method/index.js');

class Gatherer {
	constructor() {
		this._initialized = false;
		this._locked = {};
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
		console.log("lock", section, this.locked(section));
		if (this.locked(section))
			return Promise.reject(new Error(`Section ${section} is locked.`));
		console.log("lock", section, this._locked);
		_.set(this._locked, [section, 'value'], true);
		_.set(this._locked, [section, 'ts'], _.now() + this._ttl);
		console.log("lockres", section, this._locked);
	}

	lockSections(sections) {
		_.map(_.castArray(sections), sc => this.lock(sc));
	}

	unlockSections(sections) {
		_.map(_.castArray(sections), sc => this.unlock(sc));
	}

	lockEntire() {
		this.lock('_global');
	}

	unlockEntire() {
		this.unlock('_global');
	}

	unlock(section) {
		console.log("unlock", section, this._locked);
		_.set(this._locked, [section, 'value'], false);
		_.unset(this._locked, [section, 'ts']);
		console.log("unlockres", section, this._locked);
	}

	locked(section) {
		if (_.get(this._locked, [section, 'ts'], _.now() + this._ttl) < _.now()) {
			this.unlock(section);
		}
		return _.get(this._locked, [section, 'value'], false);
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
			return Promise.reject(new Error("Ain't ready to give you stats"));
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
		_.set(this.timestamp, ['_last'], _.now());
		_.unset(this, '_computed');
		if (!section) {
			this._fill(data);
		} else {
			_.set(this.timestamp, section, _.now());
			console.log("set ts", this.timestamp);
			_.set(this._dataset, section, data);
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