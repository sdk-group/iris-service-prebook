'use strict'

let events = {
	prebook: {}
}

let tasks = [];


module.exports = {
	module: require('./prebook.js'),
	name: 'prebook',
	permissions: [],
	tasks: tasks,
	exposed: true,
	events: {
		group: 'prebook',
		shorthands: events.prebook
	}
};