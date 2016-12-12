'use strict'

const BookingApi = require('resource-management-framework')
	.BookingApi;
const ServiceApi = require('resource-management-framework')
	.ServiceApi;
const Patchwerk = require('patchwerk');
const moment = require('moment-timezone');
require('moment-range');

const Gatherer = require('./stats.js');
const Collector = require('./stat-datasource/data-processor.js');

const SimplifiedMosaic = require("./simplified-mosaic.js");

const UIValidation = require("iris-user-info-validation");
var UIValidator;

var RESTRICTED_DAYS;

class Prebook {
	constructor() {
		this.emitter = message_bus;
	}
	init(config) {

		this.iris = new BookingApi();
		this.iris.initContent();
		this.services = new ServiceApi();
		this.services.initContent();

		this.patchwerk = Patchwerk(message_bus);

		UIValidator = UIValidation(this.emitter);

		Gatherer.setTransforms(['live-slots-count', 'prebook-slots-count']);

		Collector.init(this.emitter);
		Collector.setBuilder(this.iris.getCachingFactory.bind(this.iris));

		SimplifiedMosaic.init(this.patchwerk, this._mosaicSaveSlots.bind(this));

		this.prebook_check_interval = config.prebook_check_interval || 30;
		this.warmup_throttle_hours = config.warmup_throttle_hours || 24;
		this.service_quota_flag_expiry = config.service_quota_flag_expiry || 1800;
	}

	launch() {
			// this.emitter.command('taskrunner.add.task', {
			// 	time: this.prebook_check_interval,
			// 	task_name: "",
			// 	module_name: "prebook",
			// 	task_type: "add-task",
			// 	solo: true,
			// 	// regular: true,
			// 	params: {
			// 		_action: "expiration-check"
			// 	}
			// });

			this.emitter.listenTask('prebook.save.service.quota', (data) => this.actionUpdateServiceQuota(data));
			this.emitter.listenTask('prebook.recount.service.slots', (data) => this.actionRecountServiceSlots(data));

			this.emitter.on('engine.ready', () => {
				return this.actionFillGatherer({});
			});

			this.emitter.on('engine.ready', () => {
				return this._temporalLoadDayRestriction();
			});

			this.emitter.on('ticket.emit.state', (data) => {
				if (data.event_name == 'register' || (data.event_name == 'book' && data.today) || data.event_name == 'closed' || data.event_name == 'processing') {
					console.log("TICK EMIT STATE GATH");
					Gatherer.invalidate(data.ticket.org_destination);
				}
			});
			this.emitter.on('workstation.emit.change-state', (data) => {
				// console.log("GATH INVALIDATE", data);
				if (data.organization) {
					Gatherer.invalidate(data.organization);
				}
			});
			return this.actionScheduleWarmupAll()
				.then(res => true);
		}
		//API

	_temporalLoadDayRestriction() {
		return this.services.getGlobal("restricted_days")
			.then(days_by_org => {
				RESTRICTED_DAYS = days_by_org;
				return true;
			});
	}

	_temporalCheckRestrictedDay(org, day) {
		let rs = RESTRICTED_DAYS[org] || RESTRICTED_DAYS.default || [];
		return !~rs.indexOf(day);
	}

	actionFillGatherer({
		organization: organization = []
	}) {
		let org_seq;
		let now = _.now();
		let org_keys = _.filter(organization, org => !Gatherer.locked(org));
		let is_all = _.isEmpty(organization);
		console.log("FILL", is_all, organization, org_keys, Gatherer.timestamp, Gatherer._expiry, Gatherer._locked);
		if (_.isEmpty(org_keys) && !is_all)
			return Promise.reject(new Error('Not ready yet.'));
		if (is_all) {
			Gatherer.lock();
		} else {
			Gatherer.lockSections(org_keys);
		}
		let time = process.hrtime();
		return this.emitter.addTask('workstation', {
				_action: 'organization-data',
				organization: is_all ? null : org_keys,
				embed_schedules: true
			})
			.then(orgs => {
				let diff = process.hrtime(time);
				console.log('PRE FILL GATH IN %d seconds', diff[0] + diff[1] / 1e9);

				org_seq = _.values(orgs);
				return Promise.mapSeries(org_seq, (org) => this.statsByOrganization(org));
			})
			.then((res) => {
				let diff = process.hrtime(time);
				console.log('FILL GATH IN %d seconds', diff[0] + diff[1] / 1e9);
				_.map(res, (org_data, index) => {
					let section = org_seq[index].org_merged.id;
					Gatherer.update(section, org_data.data);
					Gatherer.setExpiry(section, now + org_data.expiry * 1000);
				});
				if (is_all) {
					Gatherer.unlock();
				} else {
					Gatherer.unlockSections(org_keys);
				}
				return Promise.resolve(true);
			})
			.catch(err => {
				console.log("GATH ERR", err.stack);
				if (is_all) {
					Gatherer.unlock();
				} else {
					Gatherer.unlockSections(org_keys);
				}
				return Promise.reject(err);
			});
	}

	_statsByCount(organization_data) {
		let org = organization_data;
		let stats;
		return this.patchwerk.get("TicketCounter", {
				department: org.org_merged.id,
				date: moment.tz(org.org_merged.org_timezone)
					.format("YYYY-MM-DD")
			})
			.then((res) => {
				let val = (res.getSource() || 0) + 1;
				stats = {
					forced_live_slots_count: val,
					forced_max_live_slots_count: org.org_merged.max_slots_per_day,
					forced_prebook_slots_count: val,
					forced_max_prebook_slots_count: org.org_merged.max_slots_per_day
				};


				let result = {
					"*": stats
				};
				return {
					data: result,
					expiry: 420
				};
			})
	}

	statsByOrganization(organization_data) {
		let org = organization_data;
		let stats;
		let mode = org.org_merged.workstation_resource_enabled ? 'destination' : 'operator';
		org.agent_type = mode;
		if (_.isNumber(org.org_merged.max_slots_per_day))
			return this._statsByCount(org);


		return Promise.props({
				agent_keys: (mode == 'destination' ? this.emitter.addTask('workstation', {
					_action: 'resource-keys',
					organization: org.org_merged.id,
					device_type: 'control-panel'
				}) : this.emitter.addTask('agent', {
					_action: 'resource-keys',
					role: 'Operator',
					organization: org.org_merged.id
				})),
				srv: this.services.getServiceIds(org.org_merged.id)
			})
			.then(({
				agent_keys,
				srv
			}) => {
				let tz = org.org_merged.org_timezone;
				let dedicated = moment.tz(tz);
				let now = moment.tz(tz)
					.diff(moment.tz(tz)
						.startOf('day'), 'seconds');

				let lsch = _.find(_.castArray(org.org_merged.has_schedule.live || []), (piece) => {
					return !!~_.indexOf(piece.has_day, dedicated.format('dddd'));
				});
				let psch = _.find(_.castArray(org.org_merged.has_schedule.prebook || []), (piece) => {
					return !!~_.indexOf(piece.has_day, dedicated.format('dddd'));
				});
				let lchunks = lsch ? _.flatMap(lsch.has_time_description, 'data.0') : [86400];
				let pchunks = psch ? _.flatMap(psch.has_time_description, 'data.0') : [86400];
				let ltd = [now, _.max(lchunks) || 86400];
				let ptd = [now + org.org_merged.prebook_observe_offset, _.max(pchunks) || 86400];

				return {
					org_addr: org.org_addr,
					org_merged: org.org_merged,
					org_chain: org.org_chain,
					agent_type: org.agent_type,
					agent_keys: agent_keys,
					d_date: dedicated,
					ltd: ltd,
					ptd: ptd,
					service_keys: srv
				};
			})
			.then(preprocessed => {
				org = preprocessed;
				return Collector.process({
					actor: org.agent_keys.active,
					time_description: org.ltd,
					dedicated_date: org.d_date,
					service_keys: this.services.getSystemName('registry', 'service', [org.org_merged.id]),
					actor_keys: org.agent_keys.all,
					actor_type: org.agent_type,
					organization: org.org_merged.id,
					method: 'live',
					quota_status: true,
					today: true
				});
			})
			.then((res) => {
				stats = res.stats || {};
				return Collector.process({
					actor: '*',
					time_description: org.ptd,
					dedicated_date: org.d_date,
					service_keys: this.services.getSystemName('registry', 'service', [org.org_merged.id]),
					actor_keys: org.agent_keys.all,
					actor_type: org.agent_type,
					organization: org.org_merged.id,
					method: 'prebook',
					quota_status: true,
					today: true
				});
			})
			.then((res) => {
				stats = _.merge(stats, res.stats);
				return Promise.mapSeries(org.service_keys, (service) => {
					return this.patchwerk.get('Service', {
						department: org.org_merged.id,
						counter: _.last(_.split(service, '-'))
					});
				});
			})
			.then((res) => {
				// console.log(res);
				let expiry = [];
				let date = moment.tz(org.org_merged.org_timezone)
					.format("YYYY-MM-DD");
				let dummy = {
					available: {
						live: 0,
						prebook: 0
					},
					max_available: {
						live: 0,
						prebook: 0
					},
					max_solid: {
						live: 0,
						prebook: 0
					},
					reserved: 0,
					mapping: {
						live: [],
						prebook: []
					}
				};

				let result = _.reduce(res, (acc, srv, index) => {
					//@FIXIT: proper way to determine service key
					// let success = !!(stats.max_available.live * part) && ((stats.max_available.live * part) >= (stats.reserved));
					let part = _.clamp(srv.prebook_today_percentage || 0, 0, 100) / 100;;
					// console.log("FILL SRV", _.get(stats, [org.service_keys[index], date], {}));
					let srv_data = stats[org.service_keys[index]] && stats[org.service_keys[index]][date] || {};
					_.defaultsDeep(srv_data, dummy);
					// console.log(org.org_merged.id, org.service_keys[index], _.head(stats[org.service_keys[index]]), srv_data, srv);
					acc[org.service_keys[index]] = {
						live_slot_size: srv.live_operation_time,
						prebook_slot_size: srv.prebook_operation_time,
						live_available_time: srv_data.available.live,
						prebook_available_time: srv_data.available.prebook,
						live_total_time: srv_data.max_available.live,
						prebook_total_time: srv_data.max_available.prebook,
						live_solid_time: srv_data.max_solid.live,
						prebook_solid_time: srv_data.max_solid.prebook,
						live_chunk_mapping: srv_data.mapping.live,
						prebook_chunk_mapping: srv_data.mapping.prebook,
						prebook_percentage: part,
						reserved: srv_data.reserved
					};
					expiry.push(srv.live_operation_time, srv.prebook_operation_time);

					return acc;
				}, {});
				return {
					data: result,
					expiry: _.min(expiry)
				};
			});
	}

	actionServiceStats({
		organization
	}) {
		// console.log("SERVSLOTs", Gatherer.stats(organization));
		// console.log("sslots", organization, Gatherer.expired(organization));
		if (Gatherer.expired(organization))
			return this.actionFillGatherer({
					organization: organization
				})
				.catch((err) => ({}))
				.then(res => Gatherer.stats(organization));

		return Gatherer.stats(organization);
	}


	actionExpirationCheck({
		ts_now
	}) {
		// this.emitter.command('taskrunner.add.task', {
		// 	time: this.prebook_check_interval,
		// 	task_name: "",
		// 	module_name: "prebook",
		// 	task_type: "add-task",
		// 	solo: true,
		// 	// regular: true,
		// 	params: {
		// 		_action: "expiration-check"
		// 	}
		// });
		console.log("EXP CHECK");
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
					.value();
				// console.log("TICKS TO EXPIRE", tickets, ts_now, ticks);
				return Promise.map(tickets, (ticket) => {
					console.log("EXP", ticket.id, ticket.org_destination);
					return this.emitter.addTask("queue", {
						_action: "ticket-expire",
						ticket: ticket.id,
						organization: ticket.org_destination,
						auto: true
					});
				});
			})
			.catch((err) => {
				global.logger && logger.error(
					err, {
						module: 'prebook',
						method: 'expiration-check'
					});
				console.log("EXPIRATION CHECK ERR", err.stack);
				return false;
			});
	}


	actionScheduleWarmupAll() {
		return this.emitter.addTask('workstation', {
				_action: 'organization-data',
				embed_schedules: true
			})
			.then((res) => {
				let to_warmup = _.filter(res, org => !_.isUndefined(org.org_merged.auto_warmup_time) && !_.isEmpty(org.org_merged.has_schedule));
				let times = {};
				_.map(to_warmup, (org) => {
					let t = moment.tz(org.org_merged.auto_warmup_time, 'HH:mm', org.org_merged.org_timezone);
					let tm = t.diff(moment.tz(org.org_merged.org_timezone), 'seconds') % 86400;
					if (tm <= 0) tm = 86400 + tm;
					let org_id = org.org_merged.id;
					this.emitter.addTask('taskrunner.add.task', {
						time: tm,
						task_name: "",
						ahead: false,
						solo: true,
						cancellation_code: org_id,
						module_name: "prebook",
						task_id: "auto-warmup-all",
						task_type: "add-task",
						ahead: false,
						params: {
							_action: "auto-warmup-all",
							organization: _.pick(res, org_id)
						}
					});
				});
				return true;
			});
	}

	actionAutoWarmupAll({
		organization
	}) {
		this.actionScheduleWarmupAll();
		return this.actionWarmupAll({
			organization,
			auto: true
		});
	}

	actionWarmupAll({
		organization,
		auto
	}) {
		global.logger && logger.info({
			module: 'prebook',
			method: 'warmup-all',
			organization
		}, 'Auto-warmup: ');
		return Promise.mapSeries(_.values(organization), (organization_data) => {
			return this.actionWarmupDaysCache({
					organization_data,
					start: 0,
					end: 32,
					auto: auto
				})
				.catch(err => true);
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
		let late = !today && dedicated.isBefore(moment.tz(tz), 'day');
		let start = today ? now + offset : _.min(chunks);
		let td = [start, _.max(chunks)];
		// console.log("SCH", today, td, sch, dedicated.format('dddd'));
		// console.log("DATES", dedicated_date, dedicated.format(), booking.format(), start, today);
		return {
			d_date: dedicated,
			b_date: booking.format(),
			today,
			late,
			td
		};
	}
	prepareAvailableDaysProcessing({
		workstation,
		service,
		start = 0,
		end
	}) {
		let org;
		return this.actionWorkstationOrganizationData({
				workstation,
				embed_schedules: true
			})
			.then((pre) => {
				org = pre;
				let mode = org.org_merged.workstation_resource_enabled ? 'destination' : 'operator';
				org.agent_type = mode;
				return Promise.props({
					agent_keys: mode == 'destination' ? this.emitter.addTask('workstation', {
						_action: 'resource-keys',
						organization: org.org_merged.id,
						device_type: 'control-panel'
					}) : this.emitter.addTask('agent', {
						_action: 'resource-keys',
						role: 'Operator',
						organization: org.org_merged.id
					}),
					srv: this.patchwerk.get('Service', {
						department: org.org_merged.id,
						counter: _.last(_.split(service, '-'))
					})
				});
			})
			.then(({
				agent_keys,
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
							tz: org.org_merged.org_timezone,
							offset: org.org_merged.prebook_observe_offset,
							schedules: org.org_merged.has_schedule.prebook
						});

						return {
							ws: org.ws,
							org_addr: org.org_addr,
							org_merged: org.org_merged,
							org_chain: org.org_chain,
							agent_type: org.agent_type,
							agent_keys: agent_keys,
							srv,
							service,
							d_date: dates.d_date,
							d_date_key: dates.d_date.format("YYYY-MM-DD"),
							b_date: dates.b_date,
							td: dates.td,
							today: dates.today
						};
					}),
					done
				};
			});
	}
	preparePrebookProcessing({
		workstation,
		organization,
		service,
		dedicated_date,
		offset = true
	}) {
		let org;
		return (workstation ? this.actionWorkstationOrganizationData({
					workstation: workstation,
					embed_schedules: true
				}) : this.emitter.addTask('workstation', {
					_action: 'organization-data',
					organization: organization,
					embed_schedules: true
				})
				.then(res => res[organization]))
			.then((pre) => {
				org = pre;
				let mode = org.org_merged.workstation_resource_enabled ? 'destination' : 'operator';
				org.agent_type = mode;
				return Promise.props({
					agent_keys: mode == 'destination' ? this.emitter.addTask('workstation', {
						_action: 'resource-keys',
						organization: org.org_merged.id,
						device_type: 'control-panel'
					}) : this.emitter.addTask('agent', {
						_action: 'resource-keys',
						role: 'Operator',
						organization: org.org_merged.id
					}),
					srv: this.patchwerk.get('Service', {
						department: org.org_merged.id,
						counter: _.last(_.split(service, '-'))
					})
				});
			})
			.then(({
				agent_keys,
				srv
			}) => {
				let dates = this.getDates({
					dedicated_date,
					tz: org.org_merged.org_timezone,
					offset: (offset ? org.org_merged.prebook_observe_offset : 0),
					schedules: org.org_merged.has_schedule.prebook
				});
				return {
					ws: org.ws,
					org_addr: org.org_addr,
					org_merged: org.org_merged,
					org_chain: org.org_chain,
					agent_keys: agent_keys,
					agent_type: org.agent_type,
					srv,
					service,
					d_date: dates.d_date,
					d_date_key: dates.d_date.format("YYYY-MM-DD"),
					b_date: dates.b_date,
					td: dates.td,
					today: dates.today,
					late: dates.late
				};
			});
	}

	_checkSlots(org, tickets) {
		return this.actionGetStats(org)
			.then((keyed) => {
				let pre = keyed[0];
				let s_count = tickets[0].service_count;
				let success = pre.success;
				// let count = org.org_merged.prebook_observe_max_slots || 1000
				let count = _.round((pre.available.prebook || 0) / (pre.data.srv.prebook_operation_time * s_count));
				// org = pre.data;
				// console.log("OBSERVING PREBOOK II", count, pre.available, org.org_merged.prebook_observe_max_slots || count);
				return !success ? [] : this._getOrComputeServiceSlots(org, count);
			})
			.then((cache) => {
				let placed = {},
					slot, ticket, ll = tickets.length;
				for (var i = 0; i < ll; i++) {
					ticket = tickets[i];
					let all_slots = this._constructSolidSlots(cache, ticket.service_count),
						l = all_slots.length;
					for (var j = 0; j < l; j++) {
						slot = all_slots[j];
						if (_.some(slot.from, s => s.placed))
							continue;
						if (slot.time_description[0] == ticket.time_description[0] &&
							slot.time_description[1] == ticket.time_description[1] &&
							(!ticket.operator || slot.operator == ticket.operator) &&
							(!slot.service || slot.service == ticket.service)) {
							_.map(slot.from, single_slot => {
								single_slot.placed = true
							});
							placed[i] = true;
						}
					}
				}
				console.log("##############################################################\n", tickets, _.filter(cache, 'placed'), placed);
				placed = Object.keys(placed);
				return Promise.resolve(placed.length == tickets.length);
			});
	}

	_confirm(force, org, tickets) {
		let manualObserve = org.today ? Promise.resolve(true) : (force ? Promise.resolve(true) : this._checkSlots(org, tickets));
		return manualObserve.then(approval => {
				if (!approval)
					return Promise.reject(new Error('Failed to place a ticket.'));
				return this.iris.confirm({
					actor_type: org.agent_type,
					actor: '*',
					service_keys: this.services.getSystemName('registry', 'service', [org.org_merged.id]),
					organization: org.org_merged.id,
					actor_keys: org.agent_keys.all,
					time_description: org.td,
					dedicated_date: org.d_date,
					tickets: tickets,
					method: 'prebook',
					// nocheck: !!force,
					nocheck: org.today ? !!force : true,
					today: org.today
				});
			})
			.then(res => {
				if (!_.isEmpty(res.lost) && (res.placed.length != tickets.length))
					return Promise.reject(new Error('Failed to place a ticket.'));

				let placed = res.placed || [],
					keys = _.map(placed, '@id');

				return Promise.map(placed, (tick) => {
						return this.iris.ticket_api.setCodeLookup(tick['@id'], tick.code);
					})
					.then(res => ({
						success: true,
						keys: keys
					}));
			})
			.catch((err) => {
				console.log("_CONFIRM ERR!", err.stack);
				global.logger && logger.error(
					err, {
						module: 'prebook',
						method: '_confirm'
					});
				return {
					success: false,
					reason: err.message
				};
			});
	}


	_validateConfirmArguments(data) {
		//synchronous
		let res = true;
		if (!data.time_description || data.time_description.constructor !== Array)
			res = false;

		if (!res)
			return Promise.reject(new Error("Invalid input data."));
		return UIValidator.validate(data);
	}

	_updateValidationInfo(data) {
		return UIValidator.update(data);
	}

	actionTicketConfirm(data) {
		console.log(data);
		let fnames = ['service', 'operator', 'destination', 'code', 'force', 'token',
						'org_destination', 'booking_method', 'time_description', 'label',
						'dedicated_date',
						'service_count',
						'priority',
						'user_info',
						'user_info_description',
						'workstation', 'user_id', 'user_type', '_action', 'request_id'];

		let user_info = data.user_info || _.omit(data, fnames);
		let force = !!data.force;
		let source_info = {
			time_description: data.time_description,
			service: _.take(_.castArray(data.service), 1),
			booking_method: "prebook",
			state: "booked",
			operator: data.operator,
			destination: data.destination,
			dedicated_date: data.dedicated_date,
			service_count: _.map(_.castArray(data.service_count), sc => Math.abs(parseInt(sc))),
			priority: data.priority,
			code: data.code,
			label: data.label,
			user_info: user_info,
			user_info_description: data.user_info_description
		};

		let org;
		let event_name = 'book';
		let tickets;
		let context = {};
		let count = 1;
		let s_count = source_info.service_count[0] || 1;
		let exp_diff;

		let time = process.hrtime();
		return this._validateConfirmArguments(source_info)
			.then(approval => {
				return this.preparePrebookProcessing({
					workstation: data.workstation,
					organization: data.org_destination,
					service: source_info.service[0],
					dedicated_date: data.dedicated_date,
					offset: false
				});
			})
			.then((pre) => {
				org = pre;
				// 	if (org.org_merged.max_slots_per_day)
				// 		force = true;
				// 	return org.org_merged.max_slots_per_day ? this._observeByCount(org, source_info.service)
				// 		.then(res => res.success) : true;
				// })
				// .then((approval) => {
				// 	if (org.org_merged.max_slots_per_day && !approval)
				// 		return Promise.reject("Failed to place a ticket: reached slots limit.");

				return this.emitter.addTask('history', {
					_action: 'make-entry',
					subject: {
						type: 'terminal',
						id: data.workstation
					},
					event_name,
					reason: {},
					context: {
						workstation: data.workstation
					}
				});
			})
			.then(history => {
				history.local_time = moment.tz(org.org_merged.org_timezone)
					.format();
				source_info.history = [history];
				return this.actionGetStats(org);
			})
			.then((keyed) => {
				// console.log(keyed);
				let pre = keyed[0];
				let success = pre.success;
				count = _.round((pre.available.prebook || 0) / (pre.data.srv.prebook_operation_time));
				org = pre.data;

				exp_diff = org.d_date.clone()
					.startOf('day')
					.add(data.time_description[0], 'seconds')
					.diff(moment.tz(org.org_merged.org_timezone)) + org.org_merged.prebook_expiration_interval * 1000;

				// console.log("EXPIRES IN", exp_diff, org.org_merged.prebook_expiration_interval);

				return this.emitter.addTask("taskrunner.now")
					.then((res) => (res + exp_diff));
			})
			.then((expiry) => {

				source_info.dedicated_date = org.d_date.format('YYYY-MM-DD');
				source_info.expiry = expiry;

				return this.emitter.addTask('ticket-index', {
					_action: 'confirm-session',
					source: source_info,
					org_data: org.org_merged,
					confirm: this._confirm.bind(this, force, org)
				});
			})
			.then((confirmed) => {
				// console.log("RES", require('util')
				// 	.inspect(confirmed, {
				// 		depth: null
				// 	}));
				// let diff = process.hrtime(time);
				// console.log('PB CONFIRM FIN IN %d mseconds', (diff[0] * 1e9 + diff[1]) / 1000000);
				// time = process.hrtime();
				if (!confirmed.success)
					return Promise.reject(new Error(confirmed.reason));
				tickets = confirmed.response;


				// this.emitter.command("prebook.save.service.quota", {
				// 	data: org,
				// 	reset: true
				// });
				this.emitter.command("prebook.recount.service.slots", org);
				// console.log("CONFIRMING", res);

				_.map(tickets, tick => {
					// console.log("TICKET CNF", tick);
					this.emitter.emit('ticket.emit.state', {
						org_addr: org.org_addr,
						org_merged: org.org_merged,
						today: org.today,
						ticket: tick,
						event_name: event_name,
						workstation: data.workstation
					});
					this.emitter.command('taskrunner.add.task', {
						time: exp_diff / 1000,
						task_name: "",
						module_name: "queue",
						task_type: "add-task",
						cancellation_code: tick.code,
						solo: true,
						params: {
							_action: "ticket-expire",
							ticket: tick.id,
							organization: tick.org_destination,
							auto: true
						}
					});

				});
				return this._updateValidationInfo(source_info);
			})
			.then((res) => {

				return {
					success: true,
					ticket: tickets,
					context: context
				};

			})
			.catch((err) => {
				org && this.emitter.command("prebook.recount.service.slots", org);
				console.log("PB CONFIRM ERR!", err.stack);
				global.logger && logger.error(
					err, {
						module: 'prebook',
						method: 'confirm'
					}
				);
				return {
					success: false,
					reason: err.message
				};
			});
	}


	_observeByCount(org, services) {
		return this.patchwerk.get("TicketCounter", {
				department: org.org_merged.id,
				date: org.d_date.format('YYYY-MM-DD')
			})
			.then((res) => {
				let val = res.getSource() || 0;
				if (val + 1 < org.org_merged.max_slots_per_day)
					return {
						details: [],
						success: true
					};
				else
					return {
						details: services,
						success: false
					};
			});
	}

	actionTicketObserve({
		service,
		dedicated_date,
		workstation,
		operator,
		destination,
		service_count = [1]
	}) {
		// console.log("DEDICATED OBSERVE", dedicated_date);
		let org;
		let s_count = _.castArray(service_count);
		s_count = Math.abs(_.parseInt(s_count[0])) || 1;
		let services = _.castArray(service);
		let time = process.hrtime();
		return this.preparePrebookProcessing({
				workstation: workstation,
				service: services[0],
				dedicated_date: dedicated_date,
				offset: true
			})
			.then((res) => {
				// console.log("OBSERVE WS", workstation);
				let diff = process.hrtime(time);
				console.log('PRE OBSERVE PREPARED IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();
				if (res.late)
					return Promise.reject(new Error("Dedicated date is in past."));
				org = res;
				return this.actionGetStats(res);
			})
			.then((keyed) => {
				// console.log("____________\nQUOTA", keyed);
				let diff = process.hrtime(time);
				console.log('PRE OBSERVE GOT STATS IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();
				let pre = keyed[0];
				let success = pre.success;
				// let count = org.org_merged.prebook_observe_max_slots || 1000
				let count = _.round((pre.available.prebook || 0) / (pre.data.srv.prebook_operation_time * s_count));
				// org = pre.data;
				// console.log("OBSERVING PREBOOK II", count, pre.available, org.org_merged.prebook_observe_max_slots || count);
				return !success ? [] : this.getServiceSlots({
					preprocessed: org,
					operator: operator,
					count: count,
					s_count: s_count
				});
			})
			.then((res) => {
				// console.log("RES", require('util')
				// 	.inspect(res, {
				// 		depth: null
				// 	}));
				let diff = process.hrtime(time);
				console.log('PRE OBSERVE DONE IN  %d seconds', diff[0] + diff[1] / 1e9);
				return {
					slots: res,
					success: !!_.size(res)
				};
			})
			.catch((err) => {
				console.log("PRE OBSERVE ERR!", err.stack);
				global.logger && logger.error(
					err, {
						module: 'prebook',
						method: 'observe'
					});
				return {
					success: false,
					reason: err.message
				};
			});
	}

	actionAvailableDays({
		service,
		workstation,
		operator,
		destination,
		service_count = 1,
		per_service = 1,
		start,
		end
	}) {
		let done;
		let time = process.hrtime();
		// console.log("OBSERVING AVDAYS PREBOOK", service, workstation, start, end);
		let s_count = _.castArray(service_count);
		s_count = _.parseInt(s_count[0]) || 1;
		let services = _.castArray(service);
		return this.prepareAvailableDaysProcessing({
				workstation,
				service: services[0],
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
					let local_key = pre.d_date.format();
					let success = val.success && val.max_solid.prebook && (val.max_solid.prebook >= pre.srv.prebook_operation_time * s_count, val);
					// console.log("OBSERVING PREBOOK II", success, val.success, val.max_solid.prebook, (val.max_solid.prebook >= pre.srv.prebook_operation_time * s_count));
					let cond = pre.today || this._getOrComputeServiceSlots(pre, 1)
						.then(res => !!_.size(res));
					acc[local_key] = success && cond;
					// acc[local_key] = success;
					return acc;
				}, {});
				return Promise.props(promises);
			})
			.then((res) => {
				console.log("RES", require('util')
					.inspect(res, {
						depth: null
					}));
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
				global.logger && logger.error(
					err, {
						module: 'prebook',
						method: 'available-days'
					});
				return {
					success: false,
					reason: err.message
				};
			});
	}

	actionWarmupDaysCache({
		workstation,
		organization_data,
		start,
		end,
		auto = false
	}) {
		let time = process.hrtime();
		let days;
		let org;
		let srv;
		let days_quota;

		return (organization_data ? Promise.resolve(organization_data) :
				this.actionWorkstationOrganizationData({
					workstation,
					embed_schedules: true
				}))
			.then((pre) => {
				org = pre;
				return auto ? true : this.services.serviceQuotaExpired(org.org_merged.id, this.warmup_throttle_hours * 3600000);
			})
			.then((res) => {
				// console.log("RES Q C", res);
				if (_.isBoolean(res) && !res)
					return Promise.reject(new Error('success'));
				return this.services.lockQuota(org.org_merged.id, this.service_quota_flag_expiry);
			})
			.then((res) => {
				// console.log("LOCKED");
				if (!res)
					return Promise.reject(new Error("Quota is locked."));
				logger.info('WARMUP START');

				let mode = org.org_merged.workstation_resource_enabled ? 'destination' : 'operator';
				org.agent_type = mode;
				return mode == 'destination' ? this.emitter.addTask('workstation', {
					_action: 'resource-keys',
					organization: org.org_merged.id,
					device_type: 'control-panel'
				}) : this.emitter.addTask('agent', {
					_action: 'resource-keys',
					role: 'Operator',
					organization: org.org_merged.id
				});
			})
			.then((agent_keys) => {

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
						agent_type: org.agent_type,
						agent_keys: agent_keys,
						d_date: dates.d_date,
						d_date_key: dates.d_date.format("YYYY-MM-DD"),
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
				days_quota = _.reduce(res, (acc, val) => {
					return _.merge(acc, val);
				}, {});
				// console.log(require('util')
				// 	.inspect(days_quota, {
				// 		depth: null
				// 	}));
				days_quota = _.mapValues(days_quota, (srv_q, srv_id) => {
					return _.pickBy(_.mapValues(srv_q, (date_q, date) => {
						if (!this._temporalCheckRestrictedDay(org.org_merged.id, date))
							date_q = {};
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
							max_solid: {
								live: 0,
								prebook: 0
							}
						});
					}), (data, day) => {
						// console.log("ISAFTER", day, data);
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
				global.logger && logger.info('AVDAYS CACHE WARMUP %d nanoseconds', diff[0] * 1e9 + diff[1]);

				return this._precomputeServiceSlots(days);
			})
			.then((res) => {
				return this.actionUpdateServiceQuota({
					data: days_quota,
					office: org.org_merged.id
				});
			})
			.then((res) => {
				return this.services.unlockQuota(org.org_merged.id);
			})
			.catch(err => {
				console.log("WARMUP FAILED\n<------------------------------------------->\n\n\n", err.message);
				if (err.message == 'success')
					return Promise.resolve(true);
				if (err.message !== 'Quota is locked.') {
					console.log(err.stack);
					global.logger && logger.error(
						err, {
							module: 'prebook',
							method: 'warmup'
						});
				}

				return Promise.reject(new Error("Warmup."));
			});

	}


	_precomputeServiceSlots(days) {
		return SimplifiedMosaic.observeDays(days);
	}

	_mosaicSaveSlots(office, service, date, date_key, data) {
		let expiry = date.clone()
			.add(1, 'day')
			.unix();
		// console.log("SAVING", date_key, office, service);
		return this.iris.ticket_api.cacheServiceSlots(office, service, date_key, data, {
			expiry: expiry
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

	_getOrComputeServiceSlots(preprocessed, count) {
		return this.iris.ticket_api.getServiceSlotsCache(preprocessed.org_merged.id, preprocessed.service, preprocessed.d_date_key)
			.then((res) => {
				res = res || [];
				return !_.isEmpty(res) && !preprocessed.today ? Promise.resolve(res) : this.computeServiceSlots({
						preprocessed,
						count: count
					})
					.then((computed) => _.map(computed, (t) => _.pick(t, ['time_description', 'operator', 'destination', 'source'])) || []);
			});
	}

	_constructSolidSlots(slots, s_count, operator, destination) {
		let all_slots = _.sortBy(slots, 'time_description.0');
		if (operator) {
			all_slots = _.filter(all_slots, s => s.operator == operator);
		}
		if (destination) {
			all_slots = _.filter(all_slots, s => s.destination == destination);
		}
		// console.log("SLOTS CACHE", require('util')
		// 	.inspect(all_slots, {
		// 		depth: null
		// 	}));

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
							.time_description[1]],
					from: curr
				});
			}
		}
		return solid_slots;
	}

	getServiceSlots({
		preprocessed,
		operator,
		destination,
		count,
		s_count
	}) {
		let time = process.hrtime();
		return this._getOrComputeServiceSlots(preprocessed, count)
			.then((cache) => {
				// console.log("SLOTS CACHE", require('util')
				// 	.inspect(cache, {
				// 		depth: null
				// 	}));

				let diff = process.hrtime(time);
				console.log('COMPUTED SERVICE SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();


				diff = process.hrtime(time);
				console.log('COMPOSED SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();
				let solid_slots = this._constructSolidSlots(cache, s_count, operator, destination);

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

	actionRecountServiceSlots(data) {
		let days = _.castArray(data);
		return this.actionUpdateServiceQuota({
				data: data,
				reset: true
			})
			.then(res => this._precomputeServiceSlots(days));
		// console.log("AAAAAAAAAAAAAAAAAAAAAAAA", data);
	}


	// actionUpdateServiceSlots(data) {
	// let expiry = date ? moment(date)
	// 	.add(1, 'day')
	// 	.diff(moment(), 'seconds') : 0;
	// return reset ? this.cacheServiceSlots(data) : this.iris.ticket_api.cacheServiceSlots(office, service, date, data, {
	// 	expiry
	// });
	// }


	cacheServiceSlots({
		preprocessed,
		count
	}) {
		let new_slots;
		return this.computeServiceSlots({
				preprocessed,
				count: count
			})
			.then((res) => {
				let expiry = moment(preprocessed.d_date)
					.add(1, 'day')
					.diff(moment(), 'seconds');
				new_slots = _.map(res, t => {
					return _.pick(t, ['time_description', 'operator', 'destination', 'source', 'locked_fields']);
				});
				// console.log("NEW SLOTS", require('util')
				// 	.inspect(new_slots, {
				// 		depth: null
				// 	}));

				return this.iris.ticket_api.cacheServiceSlots(preprocessed.org_merged.id, preprocessed.service, preprocessed.d_date_key, new_slots, {
					expiry
				});
			});
	}

	computeServiceSlots({
		preprocessed,
		count
	}) {
		// console.log("PRE CMP SS", preprocessed);
		return this.iris.observe({
				actor: '*',
				services: [{
					service: preprocessed.service,
					time_description: preprocessed.srv.prebook_operation_time
				}],
				time_description: preprocessed.td,
				dedicated_date: preprocessed.d_date,
				service_keys: this.services.getSystemName('registry', 'service', [preprocessed.org_merged.id]),
				actor_keys: preprocessed.agent_keys.all,
				actor_type: preprocessed.agent_type,
				organization: preprocessed.org_merged.id,
				count: count,
				service_count: 1,
				method: 'prebook',
				today: preprocessed.today
			})
			.then(res => {
				return _.filter(res, t => !!t.source);
			});
	}

	cacheServiceQuota(
		preprocessed
	) {
		let new_quota;
		let day = preprocessed.d_date.format('YYYY-MM-DD');
		let expiry = 30 * 24 * 3600;
		return this.computeServiceQuota(preprocessed)
			.then((res) => {
				new_quota = res;
				return this.services.cacheServiceQuota(preprocessed.org_merged.id, new_quota, {
					expiry
				});
			});
	}


	computeServiceQuota(preprocessed) {
		let quota = {};
		let time = process.hrtime();
		// return  this.iris.confirm({
		// 		actor: '*',
		// 		time_description: preprocessed.td,
		// 		dedicated_date: preprocessed.d_date,
		// 		service_keys: this.services.getSystemName('registry', 'service', [preprocessed.org_merged.id]),
		// 		actor_keys: preprocessed.agent_keys.all,
		// 		actor_type: preprocessed.agent_type,
		// 		organization: preprocessed.org_merged.id,
		// 		method: 'live',
		// 		quota_status: true
		// 	})
		// .then((res) => {
		// let diff = process.hrtime(time);
		// console.log('PRE COMPUTE QUOTA LIVE IN %d seconds', diff[0] + diff[1] / 1e9);
		// time = process.hrtime();

		// quota = res.stats;
		return this.iris.confirm({
				actor: '*',
				time_description: preprocessed.td,
				dedicated_date: preprocessed.d_date,
				service_keys: this.services.getSystemName('registry', 'service', [preprocessed.org_merged.id]),
				actor_keys: preprocessed.agent_keys.all,
				actor_type: preprocessed.agent_type,
				organization: preprocessed.org_merged.id,
				method: 'prebook',
				quota_status: true,
				today: preprocessed.today
					// });
			})
			.then((res) => {
				let diff = process.hrtime(time);
				console.log('PRE COMPUTE QUOTA PRE IN %d seconds', diff[0] + diff[1] / 1e9);
				return _.merge(quota, res.stats);
			});
	}


	actionGetStats(data, replace = false) {
		let days = _.castArray(data);
		let org = days[0].org_merged.id;
		let srv = days[0].service;
		let dates = _.map(days, d => d.d_date.format('YYYY-MM-DD'));
		let time = process.hrtime();
		return this.services.getServiceQuota(org, srv, dates)
			.then((quota) => {
				let diff = process.hrtime(time);
				console.log('PRE GET QUOTA IN %d seconds', diff[0] + diff[1] / 1e9);
				time = process.hrtime();
				// console.log("QUOTA", require('util')
				// 	.inspect(quota, {
				// 		depth: null
				// 	}));
				let days_missing = _.filter(days, (pre) => {
					// return true;
					// console.log(pre);
					return !_.has(quota, `${srv}.${pre.d_date_key}`) || pre.today && !!_.parseInt(days[0].srv.prebook_today_percentage);
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
							let q = _.get(res, `${srv}.${pre.d_date_key}`, {});
							_.set(res, `${srv}.${pre.d_date_key}`, q);
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
					let date = pre.d_date_key;
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
						max_solid: {
							live: 0,
							prebook: 0
						}
					});
					let success = !!(stats.max_available.prebook * part) && ((stats.max_available.prebook * part) >= (stats.reserved));
					// console.log("STATS", part, stats, `${org}.${srv}.${date}`, !!(stats.max_available.live * part) && (stats.max_available.live * part >= (stats.reserved)), stats.max_available.live * part, (stats.reserved));
					return {
						success,
						available: stats.available,
						max_solid: stats.max_solid,
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
				return result;
			});
	}
}
module.exports = Prebook;