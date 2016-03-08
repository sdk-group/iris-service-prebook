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
		return this.getTickets({
				query: {
					state: ['booked']
				}
			})
			.then((tickets) => {
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
		let dedicated = dedicated_date ? moment.tz(dedicated_date, tz) : moment.tz(tz);
		let booking = moment.utc();
		let now = moment()
			.tz(tz)
			.diff(moment()
				.tz(tz)
				.startOf('day'), 'seconds');
		let sch = _.find(_.castArray(schedules), (piece) => {
			return !!~_.indexOf(piece.has_day, dedicated.format('dddd'));
		});
		let chunks = sch ? _.flatMap(sch.has_time_description, 'data.0') : [19 * 3600];
		let p = dedicated.clone();
		let today = (booking.format("YYYY-MM-DD") === p.utc()
			.format("YYYY-MM-DD"));
		let start = today ? now + offset : _.min(chunks);
		let td = [start, _.max(chunks)];
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
				return this.getValid(res);
			})
			.then((keyed) => {
				let pre = keyed[0].data;
				// console.log("CONFIRMING PREBOOK II", pre, fields);
				org = pre;
				let prefix = _.join([pre.org_merged.prebook_label_prefix, pre.srv.prefix], '');
				prefix = !_.isEmpty(prefix) && prefix;
				let diff = pre.d_date.diff(moment.tz(pre.org_merged.org_timezone)) + pre.org_merged.prebook_expiration_interval * 1000 + time_description[0] * 1000;
				// console.log("DIFF", diff, pre.org_merged.org_timezone, pre.d_date.diff(moment.tz(pre.org_merged.org_timezone)), pre.org_merged.prebook_expiration_interval * 1000, time_description[0] * 1000, pre.d_date.format());
				return Promise.props({
					expiry: this.emitter.addTask("taskrunner.now")
						.then((res) => (res + diff)),
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
						date: pre.d_date
					})
				});
			})
			.then(({
				priority_level,
				pin,
				expiry,
				label
			}) => {
				console.log("EXPIRES IN", expiry);
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
					org_destination: org.org_merged.id,
					expiry
				};
				return this.iris.confirm({
					operator: '*',
					time_description: org.td,
					dedicated_date: org.d_date,
					service_keys: this.services.cache_service_ids,
					organization: org.org_merged.id,
					tick,
					method: 'prebook'
				});
			})
			.then((res) => {
				this.emitter.emit('history.log', {
					subject: {
						type: 'terminal',
						id: workstation
					},
					object: _.map(res.placed, "@id"),
					event_name: 'book',
					reason: {}
				});
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
				let pre = keyed[0];
				let success = pre.success;
				let count = pre.available / pre.data.srv.prebook_operation_time * service_count;
				pre = pre.data;
				// console.log("OBSERVING PREBOOK II", count, pre.org_merged.prebook_observe_max_slots || count);
				org = pre.org_merged;
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
					method: 'prebook',
					count: pre.org_merged.prebook_observe_max_slots || count,
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
				done = res.done;
				return this.getValid(res.days, true);
			})
			.then((days) => {
				// console.log("OBSERVING AVDAYS PREBOOK III", keyed);
				let diff = process.hrtime(time);
				console.log(' AVDAYS PREPARED IN %d nanoseconds', diff[0] * 1e9 + diff[1]);
				time = process.hrtime();
				let promises = _.reduce(days, (acc, val, key) => {
					let pre = val.data;
					// console.log("OBSERVING PREBOOK II", val, pre.srv.prebook_operation_time * (service_count || 1));
					let local_key = pre.d_date.format();
					acc[local_key] = val.success && (val.solid > pre.srv.prebook_operation_time * (service_count || 1));
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
				service_keys: this.services.cache_service_ids,
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
					return !_.has(quota, `${org}.${srv}.${pre.d_date.format("YYYY-MM-DD")}`);
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
				// console.log("QUOTA", days_quota);
				let preserve = [];
				let result = _.map(days, (pre) => {
					let part = (pre.today ? pre.srv.prebook_today_percentage : pre.srv.prebook_percentage);
					part = _.clamp(part, 0, 100) / 100;
					let date = pre.d_date.format("YYYY-MM-DD");
					preserve.push(date);
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
				if (replace)
					_.set(new_quota, `${org}.${srv}`, _.pick(_.get(days_quota, `${org}.${srv}`), preserve));
				// console.log("CHECKING", preserve, result, new_quota[org][srv]);
				this.emitter.emit("prebook.save.service.quota", new_quota);
				return result;
			});
	}
}
module.exports = Prebook;
