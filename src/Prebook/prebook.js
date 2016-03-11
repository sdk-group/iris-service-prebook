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
		this.prebook_check_interval = config.prebook_check_interval || 60;
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
		return this.iris.ticket_api.getExpiredTickets(ts_now)
			.then((tickets) => {
				// console.log("TICKS TO EXPIRE", tickets);
				let p = _.map(tickets, (ticket) => {
					return this.emitter.addTask("queue", {
						_action: "ticket-expire",
						ticket,
						auto: true
					});
				});
				this.emitter.emit('taskrunner.add.task', {
					now: 0,
					time: this.prebook_check_interval,
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
			})
			.then(res => res[workstation])
			.catch(err => {
				console.log("PB WSOD ERR", err.stack);
			});
	}
	getDates({
		dedicated_date,
		tz,
		offset = 600,
		schedules
	}) {
		let dedicated = dedicated_date ? moment(dedicated_date)
			.tz(tz) : moment.tz(tz);
		let booking = moment.utc();
		let now = moment()
			.tz(tz)
			.diff(moment()
				.tz(tz)
				.startOf('day'), 'seconds');
		let sch = _.find(_.castArray(schedules), (piece) => {
			return !!~_.indexOf(piece.has_day, dedicated.format('dddd'));
		});
		let chunks = sch ? _.flatMap(sch.has_time_description, 'data.0') : [86400];
		let today = booking.isSame(dedicated, 'day');
		let start = today ? now + offset : _.min(chunks);
		let td = [start, _.max(chunks)];
		// console.log("DATES", dedicated_date, dedicated.format(), booking.format(), start, today);
		return {
			d_date: dedicated,
			b_date: booking.format(),
			today,
			td
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
					.then(res => res[service])
			})
			.then(({
				org_data,
				srv
			}) => {
				let d_start = moment()
					.add(srv.prebook_offset, 'days');
				let d_end = d_start.clone()
					.add(srv.prebook_interval, 'days');
				let dates = [];
				moment.range(d_start, d_end)
					.by('days', (d) => {
						dates.push(d);
					});
				let p_start = _.clamp(start, 0, dates.length - 1);
				let p_end = _.clamp(end, 0, dates.length - 1);
				let done = (p_end == (dates.length - 1));
				let days = _.slice(dates, p_start, p_end + 1);
				return {
					days: _.map(days, (dedicated_date) => {
						let dates = this.getDates({
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
							d_date: dates.d_date,
							b_date: dates.b_date,
							td: dates.td,
							today: dates.today
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
					.then(res => res[service])
			})
			.then(({
				org_data,
				srv
			}) => {
				let dates = this.getDates({
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
					d_date: dates.d_date,
					b_date: dates.b_date,
					td: dates.td,
					today: dates.today

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
		let user_info = _.omit(fields, fnames);
		let event_name = 'book';
		let org;
		let hst;
		let b_priority;
		let s_count = _.parseInt(service_count) || 1;
		return Promise.props({
				pre: this.preparePrebookProcessing({
					workstation,
					service,
					dedicated_date,
					offset: false
				}),
				history: this.emitter.addTask('history', {
					_action: 'make-entry',
					subject: {
						type: 'terminal',
						id: workstation
					},
					event_name,
					reason: {}
				}),
				basic_priority: this.emitter.addTask('ticket', {
					_action: 'basic-priorities'
				})
			})
			.then(({
				basic_priority,
				pre,
				history
			}) => {
				hst = history;
				hst.local_time = moment.tz(pre.org_merged.org_timezone)
					.format('x');
				b_priority = basic_priority;
				return this.getValid(pre);
			})
			.then((keyed) => {
				let pre = keyed[0].data;
				org = pre;

				let diff = pre.d_date.clone()
					.startOf('day')
					.add(time_description[0], 'seconds')
					.diff(moment.tz(pre.org_merged.org_timezone)) + pre.org_merged.prebook_expiration_interval * 1000;
				// console.log("EXPIRES IN", diff);
				let prior_keys = _.keys(priority);
				let basic = _.mapValues(_.pick(b_priority, prior_keys), v => v.params);
				let local = _.pick(org.org_merged.priority_description || {}, prior_keys);
				let computed_priority = _.merge(basic, local, priority);

				let prior_prefix = _.join(_.sortedUniq(_.sortBy(_.map(computed_priority, "prefix"))), '');
				let prefix = _.join([pre.org_merged.prebook_label_prefix, prior_prefix, pre.srv.prefix], '');
				prefix = !_.isEmpty(prefix) && prefix;

				if (org.srv.priority > 0)
					computed_priority['service'] = {
						value: org.srv.priority
					};
				//+manual_up / manual_down

				return Promise.props({
					computed_priority,
					expiry: this.emitter.addTask("taskrunner.now")
						.then((res) => (res + diff)),
					pin: this.emitter.addTask('code-registry', {
						_action: 'make-pin',
						prefix: pre.org_merged.pin_code_prefix
					}),
					label: this.emitter.addTask('code-registry', {
						_action: 'make-label',
						prefix,
						date: pre.d_date.format("YYYY-MM-DD")
					})
				});
			})
			.then(({
				computed_priority,
				pin,
				expiry,
				label
			}) => {
				// console.log("EXPIRES IN ||", expiry);
				let tick = {
					dedicated_date: org.d_date,
					booking_date: org.b_date,
					time_description,
					priority: computed_priority,
					code: pin,
					user_info,
					service: org.srv.id,
					label,
					history: [hst],
					service_count: s_count,
					state: 'booked',
					called: 0,
					org_destination: org.org_merged.id,
					expiry
				};
				return this.iris.confirm({
					operator: '*',
					time_description: org.td,
					dedicated_date: org.d_date,
					service_keys: this.services.startpoint.cache_service_ids,
					organization: org.org_merged.id,
					tick,
					method: 'prebook'
				});
			})
			.then((res) => {
				this.emitter.emit("prebook.save.service.quota", {
					preprocessed: org,
					reset: true
				});
				return Promise.props({
					success: _.isEmpty(res.lost),
					tickets: this.actionTicketCompleteData({
						ticket: _.map(res.placed, "@id"),
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
		per_service = 10000
	}) {
		let org;
		let s_count = _.parseInt(service_count) || 1;
		let time = process.hrtime();
		return this.preparePrebookProcessing({
				workstation,
				service,
				dedicated_date,
				offset: true
			})
			.then((res) => {
				return this.getValid(res);
			})
			.then((keyed) => {
				let diff = process.hrtime(time);
				console.log('PRE OBSERVE PREPARED IN %d nanoseconds', diff[0] * 1e9 + diff[1]);
				time = process.hrtime();

				let pre = keyed[0];
				let success = pre.success;
				let count = _.round(pre.available / (pre.data.srv.prebook_operation_time * s_count));
				console.log("PRE TICK COUNT", count);
				pre = pre.data;
				// console.log("OBSERVING PREBOOK II", count, pre.org_merged.prebook_observe_max_slots || count);
				org = pre.org_merged;
				return !success ? {} : this.iris.observe({
					operator: '*',
					services: [{
						service: pre.srv.id,
						time_description: pre.srv.prebook_operation_time
					}],
					service_keys: this.services.startpoint.cache_service_ids,
					organization: org.id,
					time_description: pre.td,
					dedicated_date: pre.d_date,
					method: 'prebook',
					count: pre.org_merged.prebook_observe_max_slots || count,
					service_count: s_count
				});
			})
			.then((res) => {
				// console.log("RES", require('util')
				// 	.inspect(res, {
				// 		depth: null
				// 	}));
				let diff = process.hrtime(time);
				console.log('PRE OBSERVE DONE IN  %d nanoseconds', diff[0] * 1e9 + diff[1]);
				let uniq_interval = org.prebook_slot_uniq_interval || 60;
				let threshold = 0;
				let slots = _.filter(_.values(res), (tick) => {
					let eq = tick.time_description[0] < threshold;
					if (!eq) {
						threshold = tick.time_description[0] + uniq_interval;
					}
					return !eq;
				});
				slots = _.map(slots, (slot) => {
					slot.dedicated_date = _.isString(slot.dedicated_date) ? slot.dedicated_date : slot.dedicated_date.format("YYYY-MM-DD");
					return slot;
				});
				// console.log("SLOTS", require('util')
				// 	.inspect(slots, {
				// 		depth: null
				// 	}));
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
		let s_count = _.parseInt(service_count) || 1;
		return this.prepareAvailableDaysProcessing({
				workstation,
				service,
				start,
				end
			})
			.then((res) => {
				// console.log("OBSERVING AVDAYS PREBOOK II", res);
				let replace = _.parseInt(start) == 0;
				done = res.done;
				return this.getValid(res.days, replace);
			})
			.then((days) => {
				// console.log("OBSERVING AVDAYS PREBOOK III", keyed);
				let diff = process.hrtime(time);
				console.log(' AVDAYS PREPARED IN %d nanoseconds', diff[0] * 1e9 + diff[1]);
				time = process.hrtime();
				let promises = _.reduce(days, (acc, val, key) => {
					let pre = val.data;
					// console.log("OBSERVING PREBOOK II", val, pre.srv.prebook_operation_time * (_.parseInt(service_count) || 1));
					let local_key = pre.d_date.format();
					acc[local_key] = val.success && (val.solid > pre.srv.prebook_operation_time * s_count);
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
							is_available: day_data,
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
		return data.reset ? this.cacheServiceQuota(data) : this.services.cacheServiceQuota(data);
	}

	cacheServiceQuota({
		preprocessed,
		reset = false
	}) {
		let new_quota;
		return this.computeServiceQuota(preprocessed)
			.then((res) => {
				new_quota = res;
				return this.services.getServiceQuota()
			})
			.then((quota) => {
				// console.log("NEW QUOTA", new_quota[preprocessed.org_merged.id][preprocessed.srv.id]);
				return this.services.cacheServiceQuota(_.merge(quota, new_quota));
			});
	}

	computeServiceQuota(preprocessed) {
		return this.iris.confirm({
				operator: '*',
				time_description: preprocessed.td,
				dedicated_date: preprocessed.d_date,
				service_keys: this.services.startpoint.cache_service_ids,
				organization: preprocessed.org_merged.id,
				method: 'live',
				quota_status: true
			})
			.then((res) => {
				return res.stats;
			});
	}

	getValid(data, replace = false) {
		let days = _.castArray(data);
		let org = days[0].org_merged.id;
		let srv = days[0].srv.id;
		return this.services.getServiceQuota()
			.then((quota) => {
				let days_missing = _.filter(days, (pre) => {
					return !_.has(quota, `${org}.${srv}.${pre.d_date.format("YYYY-MM-DD")}`) /*|| !_.get(quota, `${org}.${srv}.${pre.d_date.format("YYYY-MM-DD")}.available`) || !_.get(quota, `${org}.${srv}.${pre.d_date.format("YYYY-MM-DD")}.solid` )*/ ;
				});
				return _.isEmpty(days_missing) ? quota : Promise.map(days_missing, (pre) => {
						return this.computeServiceQuota(pre);
					})
					.then((md) => {
						// console.log("COMPUTED", res)
						return _.reduce(md, (acc, res, index) => {
							let pre = days_missing[index];
							if (_.isEmpty(res)) {
								_.set(acc, `${org}.${srv}.${pre.d_date.format("YYYY-MM-DD")}`, {
									available: 0,
									reserved: 0,
									max_solid: 0
								});
								return acc;
							}
							return _.merge(acc, res);
						}, quota);
					});
			})
			.then((days_quota) => {
				let preserve = [];
				let result = _.map(days, (pre) => {
					let part = (pre.today ? pre.srv.prebook_today_percentage : pre.srv.prebook_percentage);
					part = _.clamp(part, 0, 100) / 100;
					let date = pre.d_date.format("YYYY-MM-DD");
					preserve.push(date);
					// console.log("QUOTA", days_quota[org][srv]);
					let stats = _.get(days_quota, `${org}.${srv}.${date}`);
					let success = (stats.available * part >= (stats.reserved + pre.srv.prebook_operation_time));
					return {
						success,
						available: stats.available,
						solid: stats.max_solid,
						data: pre
					};
				});
				let new_quota = days_quota;
				// if (replace)
				// 	_.set(new_quota, `${org}.${srv}`, _.pick(_.get(days_quota, `${org}.${srv}`), preserve));
				// console.log("CHECKING", preserve, result, new_quota[org][srv]);
				this.emitter.emit("prebook.save.service.quota", new_quota);
				return result;
			});
	}
}
module.exports = Prebook;
