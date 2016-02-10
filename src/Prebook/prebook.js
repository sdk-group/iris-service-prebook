'use strict'

let emitter = require("global-prebook");
let BookingApi = require('resource-management-framework').BookingApi;
let ServiceApi = require('resource-management-framework').ServiceApi;
let moment = require('moment-timezone');

class Prebook {
	constructor() {
		this.emitter = emitter;
	}

	init(config) {
		this.iris = new BookingApi();
		this.iris.initContent();
		this.services = new ServiceApi();
		this.services.initContent();

	}

	//API
	getTickets({
		query,
		keys
	}) {
		return this.emitter.addTask('ticket', {
				_action: 'ticket',
				query,
				keys
			})
			.then((res) => {
				// console.log("RES Q", res);
				return _.values(res);
			});
	}


	actionWorkstationOrganizationData({
		workstation,
		embed_schedules = false
	}) {
		let ws;
		return this.emitter.addTask('workstation', {
				_action: 'by-id',
				workstation
			})
			.then(res => {
				ws = _.find(res, t => (t.id == workstation || t.key == workstation));
				return embed_schedules ?
					this.services.getOrganizationSchedulesChain({
						keys: ws.attached_to
					}) : this.services.getOrganizationChain({
						keys: ws.attached_to
					});
			})
			.then((office) => {
				let org_chain = office;
				let org_merged = _.reduce(_.orderBy(_.keys(office), _.parseInt, 'desc'), (acc, val) => {
					acc = _.merge(acc, office[val]);
					return acc;
				}, {});
				let org_addr = {};
				let dept_id = _.find(office, (item) => (item.type == "Department")) || {};
				dept_id = dept_id.id;
				if(dept_id) org_addr.department = dept_id;
				let off_id = _.find(office, (item) => (item.type == "Office")) || {};
				off_id = off_id.id;
				if(off_id) org_addr.office = off_id;
				return {
					ws,
					org_addr,
					org_chain,
					org_merged
				}
			});
	}

	getDates({
		dedicated_date,
		tz,
		schedules
	}) {
		let dedicated = dedicated_date ? moment(dedicated_date) : moment();
		let booking = moment.utc();
		let day = tz ? dedicated.tz(tz).format('dddd') : dedicated.format("dddd");
		let start = tz ? moment().tz(tz).diff(moment().tz(tz).startOf('day'), 'seconds') : moment().utc().diff(moment().utc().startOf('day'), 'seconds');
		let sch = _.find(schedules, (piece) => {
			return !!~_.indexOf(piece.has_day, day);
		});
		let chunks = sch ? _.flatMap(sch.has_time_description, 'data.0') : [19 * 3600];
		let td = [start, _.max(chunks)];

		return {
			d_date: dedicated.utc().format("YYYY-MM-DD"),
			b_date: booking.format(),
			day,
			td
		};
	}

	prepareTerminalProcessing({
		workstation,
		service,
		dedicated_date
	}) {
		return Promise.props({
				org_data: this.actionWorkstationOrganizationData({
					workstation,
					embed_schedules: true
				}),
				srv: this.services.getService({
					keys: service
				})
			})
			.then(({
				org_data,
				srv
			}) => {
				let {
					d_date,
					b_date,
					td,
					day
				} = this.getDates({
					dedicated_date,
					tz: org_data.org_merged.org_timezone,
					schedules: org_data.org_merged.has_schedule
				});
				srv = _.find(srv, (t) => (t.id == service || t.key == service));
				return {
					ws: org_data.ws,
					org_addr: org_data.org_addr,
					org_merged: org_data.org_merged,
					org_chain: org_data.org_chain,
					srv,
					d_date,
					b_date,
					td,
					day
				};
			});
	}

	actionTicketConfirm(fields) {
		let fnames = ['service', 'dedicated_date', 'service_count', 'priority', 'workstation', 'user_id', 'user_type', '_action', 'request_id'];
		let {
			service,
			dedicated_date,
			service_count,
			priority,
			workstation
		} = _.pick(fields, fnames);
		let user_info = _.omit(fields, fnames);
		let org;
		let ws;
		let service_info;
		return this.prepareTerminalProcessing({
				workstation,
				service,
				dedicated_date
			})
			.then(({
				ws,
				org_addr,
				org_chain,
				org_merged,
				srv,
				d_date,
				b_date,
				td,
				day
			}) => {
				org = {
					org_chain,
					org_merged
				};
				service_info = srv;
				return Promise.props({
					td,
					d_date,
					b_date,
					srv,
					day,
					pin: this.emitter.addTask('code-registry', {
						_action: 'make-pin',
						prefix: org_merged.pin_code_prefix
					}),
					priority_level: this.emitter.addTask('ticket', {
						_action: 'compute-priority',
						priority
					}),
					label: this.emitter.addTask('code-registry', {
						_action: 'make-label',
						prefix: srv.prefix,
						date: d_date
					})
				});
			})
			.then(({
				td,
				d_date,
				b_date,
				srv,
				day,
				priority_level,
				pin,
				label
			}) => {
				let tick = {
					dedicated_date: d_date,
					booking_date: b_date,
					time_description: srv.live_operation_time,
					priority: priority_level,
					code: pin,
					user_info,
					service: srv.key,
					label,
					service_count,
					state: 'registered'
				};
				return this.iris.confirm({
					operator: '*',
					time_description: td,
					dedicated_date: d_date,
					tick,
					day
				});
			})
			.then((res) => {
				return Promise.props({
					success: _.isEmpty(res.lost),
					tickets: this.actionTicketCompleteData({
						ticket: _.keys(res.placed),
						org_chain: org.org_chain,
						service_info
					})
				});
			})
			.catch((err) => {
				console.log("PB CONFIRM ERR!", err.stack);
				return {
					success: false,
					reason: err.message
				};
			});
	}

	actionTicketCompleteData({
		ticket,
		org_chain,
		service_info
	}) {

		return Promise.props({
			ticket: this.getTickets({
				keys: ticket
			}, true),
			service: service_info,
			office: org_chain
		});
	}

	actionTicketObserve({
		service,
		dedicated_date,
		workstation,
		service_count = 1,
		per_service = 1
	}) {
		console.log("OBSERVING", service, dedicated_date);
		return Promise.props({
				pre: this.prepareTerminalProcessing({
					workstation,
					service
				})
			})
			.then(({
				pre
			}) => {
				let servs = [{
					service: pre.srv.key,
					time_description: pre.srv.live_operation_time
				}];
				return this.iris.observe({
					operator: _.keys(allowed_agent),
					services: servs,
					time_description: pre.td,
					dedicated_date: pre.d_date,
					day: pre.day,
					count: per_service,
					service_count
				});
			})
			.then((res) => {
				// console.log("RES", require('util').inspect(res, {
				// 	depth: null
				// }));
				return _.flatMap(res, _.values);
			})
			.catch((err) => {
				console.log("PRE OBSERVE ERR!", err.stack);
				return {
					success: false,
					reason: err.message
				};
			});
	}



}

module.exports = Prebook;