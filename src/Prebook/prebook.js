'use strict'

let BookingApi = require('resource-management-framework')
	.BookingApi;
let ServiceApi = require('resource-management-framework')
	.ServiceApi;
let moment = require('moment-timezone');
require('moment-range');
class Prebook {
	constructor() {
		this.emitter = message_bus;
	}
	init(config) {
		this.iris = new BookingApi();
		this.iris.initContent();
		this.services = new ServiceApi();
		this.services.initContent();
		this.prebook_check_interval = config.prebook_check_interval || 30;
		this.warmup_throttle_hours = config.warmup_throttle_hours || 1;
	}
	launch() {
			this.emitter.command('taskrunner.add.task', {
				time: this.prebook_check_interval,
				task_name: "",
				module_name: "prebook",
				task_type: "add-task",
				solo: true,
				regular: true,
				params: {
					_action: "expiration-check"
				}
			});

			this.emitter.listenTask('prebook.save.service.quota', (data) => this.actionUpdateServiceQuota(data));
			this.emitter.listenTask('prebook.save.service.slots', (data) => this.actionUpdateServiceSlots(data));
			return Promise.resolve(true);
		}
		//API
	actionExpirationCheck({
		ts_now
	}) {
		return this.emitter.addTask('workstation', {
				_action: 'organization-timezones'
			})
			.then(res => {
				let todays = _(res)
					.values()
					.uniq()
					.reduce((acc, t) => {
						acc[t] = moment.tz(t)
							.format("YYYY-MM-DD");
						return acc;
					}, {});
				return Promise.mapSeries(_.keys(res), (org_destination) => {
					return this.getTickets({
						query: {
							dedicated_date: todays[res[org_destination]],
							org_destination,
							state: ['booked']
						}
					});
				});
			})
			.then((ticks) => {
				let tickets = _(ticks)
					.flatten()
					.filter(t => (t.expiry < ts_now))
					.map('id')
					.value();
				// console.log("TICKS TO EXPIRE", tickets, ts_now, ticks);
				let p = _.map(tickets, (ticket) => {
					return this.emitter.addTask("queue", {
						_action: "ticket-expire",
						ticket,
						auto: true
					});
				});
				// this.emitter.command('taskrunner.add.task', {
				// 	time: this.prebook_check_interval,
				// 	task_name: "",
				// 	module_name: "prebook",
				// 	task_type: "add-task",
				// 	solo: true,
				// 	params: {
				// 		_action: "expiration-check"
				// 	}
				// });
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
			.then(res => res[workstation]);
	}
	getDates({
		dedicated_date,
		tz,
		offset = 600,
		schedules
	}) {
		let dedicated = dedicated_date ? moment.tz(dedicated_date, tz) : moment.tz(tz);
		let booking = moment.tz(tz);
		let now = moment.tz(tz)
			.diff(moment.tz(tz)
				.startOf('day'), 'seconds');
		let sch = _.find(_.castArray(schedules), (piece) => {
			return !!~_.indexOf(piece.has_day, dedicated.format('dddd'));
		});
		let chunks = sch ? _.flatMap(sch.has_time_description, 'data.0') : [86400];
		let today = booking.isSame(dedicated, 'day');
		let start = today ? now + offset : _.min(chunks);
		let td = [start, _.max(chunks)];
		// console.log("SCH", today, td, sch, dedicated.format('dddd'));
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
		let diff;
		let b_priority;
		let s_count = _.parseInt(service_count) || 1;
		let count = 1;
		let context = {};
		this.iris.transact();
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
					.format();
				b_priority = basic_priority;
				return this.actionGetStats(pre);
			})
			.then((keyed) => {
				let pre = keyed[0];
				let success = pre.success;
				count = _.round((pre.available.prebook || 0) / (pre.data.srv.prebook_operation_time * s_count));
				org = pre.data;

				diff = org.d_date.clone()
					.startOf('day')
					.add(time_description[0], 'seconds')
					.diff(moment.tz(org.org_merged.org_timezone)) + org.org_merged.prebook_expiration_interval * 1000;

				// console.log("EXPIRES IN", diff, org.org_merged.prebook_expiration_interval);
				let prior_keys = _.keys(priority);
				let basic = _.mapValues(_.pick(b_priority, prior_keys), v => v.params);
				let local = _.pick(org.org_merged.priority_description || {}, prior_keys);
				let computed_priority = _.merge(basic, local, priority);

				let prior_prefix = _.join(_.sortedUniq(_.sortBy(_.map(computed_priority, "prefix"))), '');
				let prefix = _.join([org.org_merged.prebook_label_prefix, prior_prefix, org.srv.prefix], '');
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
						prefix: org.org_merged.pin_code_prefix
					}),
					label: this.emitter.addTask('code-registry', {
						_action: 'make-label',
						prefix,
						office: org.org_merged.id,
						date: org.d_date.format("YYYY-MM-DD")
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
					service_keys: this.services.getSystemName('registry', 'service'),
					operator_keys: this.services.getSystemName('global', 'membership_description'),
					organization: org.org_merged.id,
					tick,
					method: 'prebook'
				});
			})
			.then((res) => {
				this.emitter.command("prebook.save.service.quota", {
					data: org,
					reset: true
				});
				this.emitter.command("prebook.save.service.slots", {
					data: {
						preprocessed: org,
						count,
						s_count
					},
					reset: true
				});
				// console.log("CONFIRMING", res);
				this.iris.endTransact();

				return Promise.props({
					lookup: Promise.map(_.values(res.placed), (tick) => {
						return this.iris.ticket_api.setCodeLookup(tick['@id'], tick.code);
					}),
					success: _.isEmpty(res.lost),
					ticket: this.getTickets({
							keys: _.map(res.placed, "@id")
						})
						.then(res => {
							if (_.isEmpty(res)) {
								return Promise.reject(new Error('Failed to place a ticket.'));
							} else {
								let tick = res[0];
								console.log("TICKET CNF", tick);

								this.emitter.command('ticket.emit.state', {
									org_addr: org.org_addr,
									ticket: tick,
									event_name,
									workstation
								});

								this.emitter.command('taskrunner.add.task', {
									time: diff / 1000,
									task_name: "",
									module_name: "queue",
									task_type: "add-task",
									cancellation_code: tick.code,
									solo: true,
									params: {
										_action: "ticket-expire",
										ticket: tick.id,
										auto: true
									}
								});
								return tick;
							}
						}),
					context
				});
			})
			.catch((err) => {
				this.iris.endTransact();
				console.log("PB CONFIRM ERR!", err.stack);
				return {
					success: false,
					reason: err.message
				};
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
		this.iris.transact();
		return this.preparePrebookProcessing({
				workstation,
				service,
				dedicated_date,
				offset: true
			})
			.then((res) => {
				let diff = process.hrtime(time);
				console.log('PRE OBSERVE PREPARED IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();

				return this.actionGetStats(res);
			})
			.then((keyed) => {
				console.log("____________\nQUOTA", keyed);
				let diff = process.hrtime(time);
				console.log('PRE OBSERVE GOT STATS IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();

				let pre = keyed[0];
				let success = pre.success;
				let count = _.round((pre.available.prebook || 0) / (pre.data.srv.prebook_operation_time * s_count));
				org = pre.data;
				// console.log("OBSERVING PREBOOK II", count, pre.available, org.org_merged.prebook_observe_max_slots || count);
				return !success ? {} : this.getServiceSlots({
					preprocessed: org,
					count,
					s_count
				});
			})
			.then((res) => {
				// console.log("RES", require('util')
				// 	.inspect(res, {
				// 		depth: null
				// 	}));
				let diff = process.hrtime(time);
				console.log('PRE OBSERVE DONE IN  %d seconds', diff[0] + diff[1] / 1e9);
				this.iris.endTransact();
				return {
					slots: res,
					success: !!_.size(res)
				};
			})
			.catch((err) => {
				console.log("PRE OBSERVE ERR!", err.stack);
				this.iris.endTransact();
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
		this.iris.transact();
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
				return this.actionGetStats(res.days, replace);
			})
			.then((days) => {
				// console.log("OBSERVING AVDAYS PREBOOK III", days);
				let diff = process.hrtime(time);
				console.log(' AVDAYS PREPARED IN %d nanoseconds', diff[0] * 1e9 + diff[1]);
				time = process.hrtime();
				let promises = _.reduce(days, (acc, val, key) => {
					let pre = val.data;
					// console.log("OBSERVING PREBOOK II", val.solid, val.success, s_count, pre.srv.prebook_operation_time, (val.solid.prebook >= pre.srv.prebook_operation_time * s_count), pre.d_date.format("YYYY-MM-DD"));
					let local_key = pre.d_date.format();
					acc[local_key] = val.success && val.available_slots.prebook && (val.available_slots.prebook >= s_count);
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
				this.iris.endTransact();
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

	actionWarmupDaysCache({
		workstation,
		start,
		end
	}) {
		let time = process.hrtime();
		let days;
		let org;
		let srv;
		this.iris.transact();
		return Promise.props({
				org: this.actionWorkstationOrganizationData({
					workstation,
					embed_schedules: true
				}),
				srv: this.services.getServiceIds()
			})
			.then((org_data) => {
				org = org_data.org;
				srv = org_data.srv;
				// console.log("RES Q C", org_data);
				return this.services.serviceQuotaExpired(org.org_merged.id, this.warmup_throttle_hours * 3600000);
			})
			.then((res) => {
				// console.log("RES Q C", res);
				if (_.isBoolean(res) && !res)
					return Promise.reject(new Error('success'));
				return this.services.lockQuota(org.org_merged.id);
			})
			.then((res) => {
				// console.log("LOCKED");
				if (!res)
					return Promise.reject(new Error("Quota is locked."));

				let d_start = moment()
					.add(start, 'days');
				let d_end = d_start.clone()
					.add(end, 'days');
				let dates = [];
				moment.range(d_start, d_end)
					.by('days', (d) => {
						dates.push(d);
					});
				// console.log("DATES", dates);
				return _.map(dates, (dedicated_date) => {
					let dates = this.getDates({
						dedicated_date,
						tz: org.org_merged.org_timezone,
						offset: org.org_merged.prebook_observe_offset,
						schedules: org.org_merged.has_schedule.prebook
					});

					return {
						ws: org.ws,
						org_addr: org.org_addr,
						org_merged: org.org_merged,
						org_chain: org.org_chain,
						d_date: dates.d_date,
						b_date: dates.b_date,
						td: dates.td,
						today: dates.today
					};
				});
			})
			.then(data => {
				days = _.castArray(data);

				// console.log("DAYS EX", days_ex, quo);

				return Promise.mapSeries(days, (pre) => {
					return this.computeServiceQuota(pre);
				});
			})
			.then((res) => {
				let min = _.minBy(days, day => day.d_date.format('x'));

				let days_quota = _.reduce(res, (acc, val) => {
					return _.merge(acc, val);
				}, {});
				// console.log(require('util')
				// 	.inspect(days_quota, {
				// 		depth: null
				// 	}));
				days_quota = _.mapValues(days_quota, (srv_q, srv_id) => {
					return _.pickBy(_.mapValues(srv_q, (date_q, date) => {
						return _.defaultsDeep(date_q, {
							max_available: {
								live: 0,
								prebook: 0
							},
							available: {
								live: 0,
								prebook: 0
							},
							reserved: 0,
							available_slots: {
								live: 0,
								prebook: 0
							}
						});
					}), (data, day) => {
						// console.log("ISAFTER", day, moment.tz(day, min.org_merged.org_timezone)
						// 	.isAfter(moment.tz(min.org_merged.org_timezone)));
						return moment.tz(day, min.org_merged.org_timezone)
							.isAfter(moment.tz(min.org_merged.org_timezone), 'day');
					});
				});

				// console.log(require('util')
				// 	.inspect(days_quota, {
				// 		depth: null
				// 	}));
				let diff = process.hrtime(time);
				console.log('AVDAYS CACHE WARMUP %d nanoseconds', diff[0] * 1e9 + diff[1]);

				this.emitter.command("prebook.save.service.quota", {
					data: days_quota,
					office: org.org_merged.id
				});
				this.iris.endTransact();
				return this.services.unlockQuota(org.org_merged.id);
			})
			.catch(err => {
				this.iris.endTransact();
				console.log("WARMUP FAILED", err.message);
				if (err.message == 'success')
					return Promise.resolve(true);
				return Promise.reject(new Error("Warmup."));
			});

	}

	actionUpdateServiceQuota({
		data,
		reset,
		office
	}) {
		let expiry = 30 * 24 * 3600;
		return reset ? this.cacheServiceQuota(data) : this.services.cacheServiceQuota(office, data, {
			expiry
		});
	}

	getServiceSlots({
		preprocessed,
		count,
		s_count
	}) {
		let time = process.hrtime();
		return this.iris.ticket_api.getServiceSlotsCache(preprocessed.org_merged.id, preprocessed.srv.id, preprocessed.d_date.format("YYYY-MM-DD"))
			.then((res) => {

				let diff = process.hrtime(time);
				console.log('GET SERVICE SLOTS CACHE IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();

				res = res || [];
				return !_.isEmpty(res) && !preprocessed.today ? Promise.resolve(res) : this.computeServiceSlots({
						preprocessed,
						count: count * s_count,
						s_count: 1
					})
					.then((computed) => {
						// console.log("COMPUTED", computed);
						return _.map(computed, (t) => _.pick(t, ['time_description', 'operator', 'destination', 'source'])) || [];
					});
			})
			.then((cache) => {
				// console.log("SLOTS CACHE", require('util')
				// 	.inspect(cache, {
				// 		depth: null
				// 	}));

				let diff = process.hrtime(time);
				console.log('COMPUTED SERVICE SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();


				this.emitter.command("prebook.save.service.slots", {
					office: preprocessed.org_merged.id,
					service: preprocessed.srv.id,
					date: preprocessed.d_date.format("YYYY-MM-DD"),
					data: cache
				});
				let all_slots = cache;

				let solid_slots = [];
				let curr = [];
				let all = _.round(_.size(all_slots) / s_count) + 1;
				// console.log("ALL SLOTS", all_slots, all);

				for (var i = 0; i < all; i++) {
					if (_.size(all_slots) < s_count)
						break;
					curr = [_.head(all_slots)];
					all_slots = _.tail(all_slots);
					// console.log("SZ ALL", _.size(all_slots), curr);
					for (var j = 0; j < s_count - 1; j++) {
						let last = _.last(curr);
						let next = _.findIndex(all_slots, t => (t.time_description[0] == last.time_description[1] && t.source == last.source));
						// console.log("NXT", last, next, !!~next, j, curr, _.size(curr));
						if (!!~next)
							curr = _.concat(curr, _.pullAt(all_slots, next));
						else
							break;
					}
					// console.log("AAAAAA", _.size(curr), s_count);
					if (_.size(curr) == s_count) {
						// console.log("PUSHING");
						solid_slots.push({
							time_description: [_.head(curr)
							.time_description[0],
							_.last(curr)
							.time_description[1]]
						});
					}
				}


				diff = process.hrtime(time);
				console.log('COMPOSED SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();


				// console.log("SOLID SLOTS", solid_slots);
				let uniq_interval = preprocessed.org_merged.prebook_slot_uniq_interval || 60;
				let threshold = 0;
				let slots = _.filter(solid_slots, (tick) => {
					let eq = tick.time_description[0] < threshold;
					if (!eq) {
						threshold = tick.time_description[0] + uniq_interval;
					}
					return !eq;
				});

				diff = process.hrtime(time);
				console.log('UNIQ SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();


				// console.log("UNIQ SLOTS", slots);
				return slots;
			});
	}

	actionUpdateServiceSlots({
		data,
		reset,
		office,
		service,
		date
	}) {
		let expiry = date ? moment(date)
			.add(1, 'day')
			.diff(moment(), 'seconds') : 0;
		return reset ? this.cacheServiceSlots(data) : this.iris.ticket_api.cacheServiceSlots(office, service, date, data, {
			expiry
		});
	}

	cacheServiceSlots({
		preprocessed,
		count,
		s_count
	}) {
		let new_slots;
		return this.computeServiceSlots({
				preprocessed,
				count: s_count * count,
				s_count: 1
			})
			.then((res) => {
				let expiry = moment(preprocessed.d_date)
					.add(1, 'day')
					.diff(moment(), 'seconds');
				new_slots = _.map(res, t => {
					return _.pick(t, ['time_description', 'operator', 'destination', 'source']);
				});
				// console.log("NEW SLOTS", require('util')
				// 	.inspect(new_slots, {
				// 		depth: null
				// 	}));

				return this.iris.ticket_api.cacheServiceSlots(preprocessed.org_merged.id, preprocessed.srv.id, preprocessed.d_date.format("YYYY-MM-DD"), new_slots, {
					expiry
				});
			});
	}

	computeServiceSlots({
		preprocessed,
		count,
		s_count
	}) {
		// console.log("PRE CMP SS", preprocessed);
		this.iris.transact();
		return this.iris.observe({
				operator: '*',
				services: [{
					service: preprocessed.srv.id,
					time_description: preprocessed.srv.prebook_operation_time
				}],
				time_description: preprocessed.td,
				dedicated_date: preprocessed.d_date,
				service_keys: this.services.getSystemName('registry', 'service'),
				operator_keys: this.services.getSystemName('global', 'membership_description'),
				organization: preprocessed.org_merged.id,
				count,
				service_count: s_count,
				method: 'prebook'
			})
			.then(res => {
				this.iris.endTransact();
				return _.values(res)
			});
	}

	cacheServiceQuota(
		preprocessed
	) {
		this.iris.transact();
		let new_quota;
		let day = preprocessed.d_date.format('YYYY-MM-DD');
		let expiry = 30 * 24 * 3600;
		return this.computeServiceQuota(preprocessed)
			.then((res) => {
				new_quota = res;
				this.iris.endTransact();
				// console.log("NEW QUOTA", new_quota[preprocessed.org_merged.id][preprocessed.srv.id]);
				return this.services.cacheServiceQuota(preprocessed.org_merged.id, new_quota, {
					expiry
				});
			});
	}


	computeServiceQuota(preprocessed) {
		let quota;
		let time = process.hrtime();
		return this.iris.confirm({
				operator: '*',
				time_description: preprocessed.td,
				dedicated_date: preprocessed.d_date,
				service_keys: this.services.getSystemName('registry', 'service'),
				operator_keys: this.services.getSystemName('global', 'membership_description'),
				organization: preprocessed.org_merged.id,
				method: 'live',
				quota_status: true
			})
			.then((res) => {
				let diff = process.hrtime(time);
				console.log('PRE COMPUTE QUOTA LIVE IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();

				quota = res.stats;
				return this.iris.confirm({
					operator: '*',
					time_description: preprocessed.td,
					dedicated_date: preprocessed.d_date,
					service_keys: this.services.getSystemName('registry', 'service'),
					operator_keys: this.services.getSystemName('global', 'membership_description'),
					organization: preprocessed.org_merged.id,
					method: 'prebook',
					quota_status: true
				});
			})
			.then((res) => {
				let diff = process.hrtime(time);
				console.log('PRE COMPUTE QUOTA PRE IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();
				// console.log("QUOT", quota, res);
				return _.merge(quota, res.stats);
			});
	}

	actionServiceStats({
		workstation,
		service,
		date
	}) {
		let org;
		let dates = date ? moment(date)
			.format('YYYY-MM-DD') : moment()
			.format('YYYY-MM-DD');
		return Promise.props({
				pre: this.actionWorkstationOrganizationData({
					workstation,
					embed_schedules: true
				}),
				srv: service ? Promise.resolve(_.castArray(service)) : this.services.getServiceIds()
			})
			.then(({
				pre,
				srv
			}) => {
				org = pre;
				return this.services.getServiceQuota(org.org_merged.id, srv, dates);
			})
			.then((res) => res[org.org_merged.id]);
	}

	actionGetStats(data, replace = false) {
		let days = _.castArray(data);
		let org = days[0].org_merged.id;
		let srv = days[0].srv.id;
		let dates = _.map(days, d => d.d_date.format('YYYY-MM-DD'));
		let time = process.hrtime();
		this.iris.transact();
		return this.services.getServiceQuota(org, srv, dates)
			.then((quota) => {
				let diff = process.hrtime(time);
				console.log('PRE GET QUOTA IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();
				console.log("QUOTA", require('util')
					.inspect(quota, {
						depth: null
					}));
				let days_missing = _.filter(days, (pre) => {
					// return true;
					return !_.has(quota, `${srv}.${pre.d_date.format("YYYY-MM-DD")}`) || pre.today;
				});
				return _.isEmpty(days_missing) ? quota : Promise.mapSeries(days_missing, (pre) => {
						return this.computeServiceQuota(pre);
					})
					.then((md) => {
						let diff = process.hrtime(time);
						console.log('PRE PRECOMPUTE QUOTA IN %d seconds', diff[0] + diff[1] / 1e9);
						time = process.hrtime();
						// console.log("MISSING", require('util')
						// 	.inspect(md, {
						// 		depth: null
						// 	}));
						return _.reduce(md, (acc, res, index) => {
							let pre = days_missing[index];
							let q = _.get(res, `${srv}.${pre.d_date.format("YYYY-MM-DD")}`, {});
							_.set(res, `${srv}.${pre.d_date.format("YYYY-MM-DD")}`, q);
							// console.log("MISSING", acc, pre.d_date.format("YYYY-MM-DD"));
							return _.merge(acc, res);
						}, quota || {});
					});
			})
			.then((days_quota) => {
				let diff = process.hrtime(time);
				console.log('PRE COMPUTE QUOTA IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();

				// console.log("QUOTA", require('util')
				// 	.inspect(days_quota, {
				// 		depth: null
				// 	}));
				let preserve = [];
				let result = _.map(days, (pre) => {
					let part = (pre.today ? pre.srv.prebook_today_percentage : pre.srv.prebook_percentage);
					part = _.clamp(part, 0, 100) / 100;
					let date = pre.d_date.format("YYYY-MM-DD");
					preserve.push(date);
					let stats = _.get(days_quota, `${srv}.${date}`);
					_.defaultsDeep(stats, {
						max_available: {
							live: 0,
							prebook: 0
						},
						available: {
							live: 0,
							prebook: 0
						},
						reserved: 0,
						available_slots: {
							live: 0,
							prebook: 0
						}
					});
					let success = !!(stats.max_available.live * part) && ((stats.max_available.live * part) >= (stats.reserved));
					// console.log("STATS", part, stats, `${org}.${srv}.${date}`, !!(stats.max_available.live * part) && (stats.max_available.live * part >= (stats.reserved)), stats.max_available.live * part, (stats.reserved));
					return {
						success,
						available: stats.available,
						available_slots: stats.available_slots,
						data: pre
					};
				});
				let new_quota = days_quota;
				if (replace) {
					let min = _.minBy(days, day => day.d_date.format('x'));
					_.set(new_quota, `${srv}`, _.pickBy(_.get(days_quota, `${srv}`), (data, day) => {
						return moment.tz(day, min.org_merged.org_timezone)
							.isAfter(moment(min.d_date), 'day') || !!~_.indexOf(preserve, day);
					}));
				}
				// console.log("CHECKING", preserve);
				// console.log(require('util')
				// 	.inspect(new_quota[org][srv], {
				// 		depth: null
				// 	}));
				diff = process.hrtime(time);
				console.log('PRE CONSTRUCT QUOTA IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();

				this.emitter.command("prebook.save.service.quota", {
					data: new_quota,
					office: org
				});
				this.iris.endTransact();
				return result;
			});
	}
}
module.exports = Prebook;