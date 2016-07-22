'use strict'

let DataSource = require('./datasource.js');

class Processor {
	setBuilder(factory_builder) {
		this.builder = factory_builder;
		this.factory = factory_builder(DataSource);
	}

	process(data) {
		console.log("PROCESS--------------------------------------->");
		data.reserve = false;
		return this.factory.getAtom(['<namespace>builder', 'box'])
			.save(data);
	}

	flush() {

	}

}

let instance = new Processor();
module.exports = instance;