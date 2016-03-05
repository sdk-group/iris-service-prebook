'use strict'
let emitter = require("global-queue");
let BookingApi = require('resource-management-framework')
	.BookingApi;
let ServiceApi = require('resource-management-framework')
	.ServiceApi;
let moment = require('moment-timezone');
require('moment-range');
class Prebook {
	constructor() {
		this.emitter = emitter;
	}
	init(config) {
		this.iris = new BookingApi();
		this.iris.initContent();
		this.services = new ServiceApi();
		this.services.initContent();
		this.prebook_check_interval = config.prebook_check_interval || 5;
	}
	launch() {
			this.emitter.emit('taskrunner.add.task', {
				now: 0,
				time: 0,
				task_name: "",
				module_name: "prebook",
				task_id: "prebook-expiration-check",
				regular: true,
				task_type: "add-task",
				params: {
					_action: "expiration-check"
				}
			});

			this.emitter.on('prebook.save.service.quota', (data) => this.updateServiceQuota(data));
			return Promise.resolve(true);
		}
		//API
	actionExpirationCheck({
		ts_now
	}) {
		return this.getTickets({
				query: {
					state: ['booked']
				}
			})
			.then((tickets) => {
				// console.log("TICKS", tickets);
				let now = ts_now / 1000;
				let min_exp = now + this.prebook_check_interval;
				let p = _.map(tickets, (tick) => {
					if (tick.expiry <= ts_now) {
						return this.emitter.addTask("queue", {
							_action: "ticket-expire",
							ticket: tick.id,
							auto: true
						})
					} else {
						return Promise.resolve(tick);
					}
				});
				// console.log("PREBOOK SCH", min_exp, now, min_exp - now);
				this.emitter.emit('taskrunner.add.task', {
					now,
					time: min_exp,
					task_name: "",
					module_name: "prebook",
					task_type: "add-task",
					task_id: "prebook-expiration-check",
					regular: true,
					params: {
						_action: "expiration-check"
					}
				});
				return Promise.all(p);
			})
			.catch((err) => {
				console.log("EXPIRATION CHECK ERR", err.stack);
				return false;
			});
	}
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
				// console.log("RES Q", res, query);
				return _.values(res);
			});
	}
	actionWorkstationOrganizationData({
		workstation,
		embed_schedules = false
	}) {
		return this.emitter.addTask('workstation', {
			_action: 'workstation-organization-data',
			workstation,
			embed_schedules
		});
	}
	getDates({
		dedicated_date,
		tz,
		offset = 600,
		schedules
	}) {
		let dedicated = dedicated_date ? moment.utc(dedicated_date) : moment.utc();
		let booking = moment.utc();
		let plan = dedicated.clone();
		plan.tz(tz);
		let day = tz ? plan.format('dddd') : dedicated.format("dddd");
		let now = tz ? moment()
			.tz(tz)
			.diff(moment()
				.tz(tz)
				.startOf('day'), 'seconds') : moment()
			.utc()
			.diff(moment()
				.utc()
				.startOf('day'), 'seconds');
		let sch = _.find(schedules, (piece) => {
			return !!~_.indexOf(piece.has_day, day);
		});
		let chunks = sch ? _.flatMap(sch.has_time_description, 'data.0') : [19 * 3600];
		let today = (booking.format("YYYY-MM-DD") === dedicated.format("YYYY-MM-DD"));
		let start = today ? now + offset : _.min(chunks);
		let td = [start, _.max(chunks)];
		return {
			d_date: dedicated.format("YYYY-MM-DD"),
			b_date: booking.format(),
			p_date: plan.format("YYYY-MM-DD"),
			day,
			td,
			now,
			today
		};
	}
	prepareAvailableDaysProcessing({
		workstation,
		service,
		start = 0,
		end
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
				srv = _.find(srv, (t) => (t.id == service || t.key == service));
				let d_start = moment.utc()
					.add(srv.prebook_offset, 'days');
				let d_end = d_start.clone()
					.utc()
					.add(srv.prebook_interval, 'days');
				let dates = [];
				moment.range(d_start, d_end)
					.by('days', (d) => {
						dates.push(d.format());
					});
				let p_start = _.clamp(start, 0, dates.length - 1);
				let p_end = _.clamp(end, 0, dates.length - 1);
				let done = (p_end == (dates.length - 1));
				let days = _.slice(dates, p_start, p_end + 1);
				return {
					days: _.map(days, (dedicated_date) => {
						let {
							d_date,
							b_date,
							p_date,
							td,
							day,
							today
						} = this.getDates({
							dedicated_date,
							tz: org_data.org_merged.org_timezone,
							offset: org_data.org_merged.prebook_observe_offset,
							schedules: org_data.org_merged.has_schedule.prebook
						});
						return {
							ws: org_data.ws,
							org_addr: org_data.org_addr,
							org_merged: org_data.org_merged,
							org_chain: org_data.org_chain,
							srv,
							d_date,
							b_date,
							p_date,
							td,
							day,
							today
						};
					}),
					done
				};
			})
			.catch(err => {
				console.log("PREBOOK TERM PREPARE ERR", err.stack);
			});
	}
	preparePrebookProcessing({
		workstation,
		service,
		dedicated_date,
		offset = true
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
				srv = _.find(srv, (t) => (t.id == service || t.key == service));
				let {
					d_date,
					b_date,
					p_date,
					td,
					day,
					today
				} = this.getDates({
					dedicated_date,
					tz: org_data.org_merged.org_timezone,
					offset: (offset ? org_data.org_merged.prebook_observe_offset : 0),
					schedules: org_data.org_merged.has_schedule.prebook
				});
				return {
					ws: org_data.ws,
					org_addr: org_data.org_addr,
					org_merged: org_data.org_merged,
					org_chain: org_data.org_chain,
					srv,
					d_date,
					b_date,
					p_date,
					td,
					day,
					today
				};
			})
			.catch(err => {
				console.log("PREBOOK TERM PREPARE ERR", err.stack);
			});
	}
	actionTicketConfirm(fields) {
		let fnames = ['service', 'dedicated_date', 'service_count', 'priority', 'workstation', 'user_id', 'user_type', '_action', 'request_id', 'time_description'];
		let {
			service,
			dedicated_date,
			service_count,
			priority,
			workstation,
			time_description
		} = _.pick(fields, fnames);
		service_count = (service_count > 0) ? service_count : 1;
		let user_info = _.omit(fields, fnames);
		let org;
		return this.preparePrebookProcessing({
				workstation,
				service,
				dedicated_date,
				offset: false
			})
			.then((res) => {
				let keyed = _.keyBy([res], 'p_date');
				return this.getValid(keyed);
			})
			.then((keyed) => {
				let pre = _.sample(keyed)
					.data;
				// console.log("CONFIRMING PREBOOK II", pre, fields);
				org = pre;
				let prefix = _.join([pre.org_merged.prebook_label_prefix, pre.srv.prefix], '');
				prefix = !_.isEmpty(prefix) && prefix;
				return Promise.props({
					expiry: this.emitter.addTask("taskrunner.now")
						.then((res) => (res + pre.org_merged.prebook_expiration_interval * 1000)),
					pin: this.emitter.addTask('code-registry', {
						_action: 'make-pin',
						prefix: pre.org_merged.pin_code_prefix
					}),
					priority_level: this.emitter.addTask('ticket', {
						_action: 'compute-priority',
						priority
					}),
					label: this.emitter.addTask('code-registry', {
						_action: 'make-label',
						prefix,
						date: pre.p_date
					})
				});
			})
			.then(({
				priority_level,
				pin,
				expiry,
				label
			}) => {
				let tick = {
					dedicated_date: org.d_date,
					booking_date: org.b_date,
					time_description,
					priority: priority_level,
					code: pin,
					user_info,
					service: org.srv.id,
					label,
					service_count,
					state: 'booked',
					called: 0,
					destination: org.org_merged.id,
					expiry
				};
				return this.iris.confirm({
					operator: '*',
					time_description: org.td,
					dedicated_date: org.d_date,
					local_date: org.p_date,
					service_keys: this.services.cache_service_ids,
					organization: org.org_merged.id,
					tick,
					method: 'prebook',
					day: org.day
				});
			})
			.then((res) => {
				this.emitter.emit('history.log', {
					subject: {
						type: 'terminal',
						id: workstation
					},
					object: _.keys(res.placed),
					event_name: 'book',
					reason: {}
				});
				this.emitter.emit("prebook.save.service.quota", {
					transform: true,
					office: org.org_merged.id,
					service: org.srv.id,
					date: org.p_date,
					transform: {
						diff: time_description[1] - time_description[0],
						increment: true
					}
				});
				return Promise.props({
					success: _.isEmpty(res.lost),
					tickets: this.actionTicketCompleteData({
						ticket: _.keys(res.placed),
						org_chain: org.org_chain,
						service_info: org.srv
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
		per_service = 100
	}) {
		let org;
		return this.preparePrebookProcessing({
				workstation,
				service,
				dedicated_date,
				offset: true
			})
			.then((res) => {
				let keyed = _.keyBy([res], 'p_date');
				return this.getValid(keyed);
			})
			.then((keyed) => {
				let pre = _.sample(keyed);
				let success = pre.success;
				pre = pre.data;
				org = pre.org_merged;
				// console.log("OBSERVING PREBOOK II", pre);
				return !success ? {} : this.iris.observe({
					operator: '*',
					services: [{
						service: pre.srv.id,
						time_description: pre.srv.prebook_operation_time
					}],
					service_keys: this.services.cache_service_ids,
					organization: org.id,
					time_description: pre.td,
					dedicated_date: pre.d_date,
					local_date: pre.p_date,
					day: pre.day,
					method: 'prebook',
					count: pre.org_merged.prebook_observe_max_slots || per_service,
					service_count
				});
			})
			.then((res) => {
				// console.log("RES", require('util')
				// 	.inspect(res, {
				// 		depth: null
				// 	}));
				let uniq_interval = org.prebook_slot_uniq_interval || 60;
				let threshold = 0;
				let slots = _.filter(_.values(res), (tick) => {
					let eq = tick.time_description[0] < threshold;
					if (!eq) {
						threshold = tick.time_description[0] + uniq_interval;
					}
					return !eq;
				});
				return {
					slots,
					success: true
				};
			})
			.catch((err) => {
				console.log("PRE OBSERVE ERR!", err.stack);
				return {
					success: false,
					reason: err.message
				};
			});
	}
	actionAvailableDays({
		service,
		workstation,
		service_count = 1,
		per_service = 1,
		start,
		end
	}) {
		let done;
		let time = process.hrtime();
		// console.log("OBSERVING AVDAYS PREBOOK", service, workstation, start, end);
		return this.prepareAvailableDaysProcessing({
				workstation,
				service,
				start,
				end
			})
			.then((res) => {
				// console.log("OBSERVING AVDAYS PREBOOK II", res);
				let keyed = _.keyBy(res.days, 'p_date');
				done = res.done;
				return this.getValid(keyed);
			})
			.then((keyed) => {
				// console.log("OBSERVING AVDAYS PREBOOK III", keyed);
				let diff = process.hrtime(time);
				console.log(' AVDAYS PREPARED IN %d nanoseconds', diff[0] * 1e9 + diff[1]);
				time = process.hrtime();
				let promises = _.reduce(keyed, (acc, val, key) => {
					// console.log("OBSERVING PREBOOK II", pre);
					let pre = val.data;
					let local_key = moment.utc(key)
						.tz(pre.org_merged.org_timezone)
						.format();
					acc[local_key] = !val.success ? {} : this.iris.observe({
						operator: '*',
						services: [{
							service: pre.srv.id,
							time_description: pre.srv.prebook_operation_time
						}],
						time_description: pre.td,
						dedicated_date: pre.d_date,
						service_keys: this.services.cache_service_ids,
						organization: pre.org_merged.id,
						local_date: pre.p_date,
						day: pre.day,
						method: 'prebook',
						count: per_service,
						service_count: (service_count > 0 ? service_count : 1)
					});
					return acc;
				}, {});
				return Promise.props(promises);
			})
			.then((res) => {
				// console.log("RES", require('util')
				// 	.inspect(res, {
				// 		depth: null
				// 	}));
				let diff = process.hrtime(time);
				console.log('AVDAYS DONE IN  %d nanoseconds', diff[0] * 1e9 + diff[1]);
				return {
					success: true,
					done,
					days: _.map(res, (day_data, day) => {
						return {
							is_available: !_.isEmpty(_.values(day_data)),
							date: day
						};
					})
				};
			})
			.catch((err) => {
				console.log("PRE OBSERVE ERR!", err.stack);
				return {
					success: false,
					reason: err.message
				};
			});
	}

	updateServiceQuota(data) {
		return data.transform ? this.mergeServiceQuota(data) : this.services.cacheServiceQuota(data);
	}

	mergeServiceQuota({
		office,
		service,
		date,
		transform
	}) {
		return this.services.getServiceQuota()
			.then((quota) => {
				let new_data = _.get(quota, `${office}.${service}.${date}`);
				let diff = transform.diff || 0;
				if (transform.increment) {
					new_data.available = new_data.available - diff;
					new_data.reserved = new_data.reserved + diff;
				} else {
					new_data.available = new_data.available + diff;
					new_data.reserved = new_data.reserved - diff;
				}
				_.set(quota, `${office}.${service}.${date}`, new_data);
				return this.services.cacheServiceQuota(quota);
			});
	}

	getValid(keyed) {
		let old_cache;
		return this.services.getServiceQuota()
			.then((quota) => {
				old_cache = quota;
				let promises = _.reduce(keyed, (acc, pre, key) => {
					let today = _.get(quota, `${pre.org_merged.id}.${pre.srv.id}.${pre.p_date}`, false);
					acc[key] =
						Promise.props({
							reserved: today ? today.reserved : this.getTickets({
									query: {
										dedicated_date: pre.d_date,
										service: pre.srv.id,
										state: pre.ws.prebook_autoregister ? 'registered' : 'booked',
										destination: pre.org_merged.id
									}
								})
								.then((tickets) => {
									return _.reduce(tickets, (acc, tick) => {
										if (_.isArray(tick.time_description))
											acc += (tick.time_description[1] - tick.time_description[0]);
										return acc;
									}, 0);
								}),
							available: today ? today.available : this.iris.getAllPlansLength({
								operator: '*',
								service: pre.srv.id,
								day: pre.day,
								local_date: pre.p_date,
								time_description: pre.td,
								service_keys: this.services.cache_service_ids,
								organization: pre.org_merged.id,
								method: 'live'
							}),
							pre
						});
					return acc;
				}, {});
				return Promise.props(promises);
			})
			.then((keyed) => {
				let cached = {};
				let result = _.reduce(keyed, (acc, {
					reserved,
					available,
					pre
				}, key) => {
					let part = (pre.today ? pre.srv.prebook_today_percentage : pre.srv.prebook_percentage);
					part = _.clamp(part, 0, 100) / 100;
					let success = (available * part >= (reserved + pre.srv.prebook_operation_time));
					if (success) {
						//only for the first time; dynamic cache creation
						_.set(cached, `${pre.org_merged.id}.${pre.srv.id}.${pre.p_date}`, {
							available,
							reserved
						});
					}
					acc[key] = {
						success,
						data: pre
					};
					// console.log("CHECKING", key, (tick_length + pre.srv.prebook_operation_time), plans * part, part);
					return acc;
				}, {});
				this.emitter.emit("prebook.save.service.quota", _.merge(old_cache, cached));
				return result;
			});
	}
}
module.exports = Prebook;
