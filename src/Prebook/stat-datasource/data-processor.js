'use strict'

let DataSource = require('./data-source.js');

class Collector {
	init(emitter) {
		this.emitter = emitter;
		this.emitter.on('inmemory-stats.flush', (query) => {
			this.flush();
		});
	}

	setBuilder(factory_builder) {
		this.builder = factory_builder;
		let built = factory_builder(DataSource);
		this.factory = built.factory;
		this.datasource = built.datasource;
	}

	process(data) {
		// console.log("PROCESS--------------------------------------->");
		data.reserve = false;
		return this.factory.getAtom(['<namespace>builder', 'box'])
			.save(data);
	}

	flush() {
		this.datasource && this.datasource.flush();
	}

}

let instance = new Collector();
module.exports = instance;