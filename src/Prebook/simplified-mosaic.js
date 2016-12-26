'use strict';

function _isArray(val) {
	return val && (val.constructor == Array);
}

function insertTick(plan, tick_td, sc = 1) {
	let l = plan.length,
		success = false,
		tick;
	for (var i = 1; i < l; i = i + 2) {
		tick = _isArray(tick_td) ? tick_td : [plan[i - 1], plan[i - 1] + parseInt(tick_td) * parseInt(sc)];
		// console.log("TTICK TD", tick_td, tick);
		if (plan[i] > tick[1] && plan[i - 1] < tick[0]) {
			plan.splice(i, 0, tick[0], tick[1]);
			success = true;
			break;
		}
		if (plan[i] == tick[1] && plan[i - 1] < tick[0]) {
			plan[i] = tick[0];
			success = true;
			break;
		}
		if (plan[i] > tick[1] && plan[i - 1] == tick[0]) {
			plan[i - 1] = tick[1];
			success = true;
			break;
		}
		if (plan[i] == tick[1] && plan[i - 1] == tick[0]) {
			plan.splice(i - 1, 2);
			success = true;
			break;
		}
	}
	return success;
}

function intersect(c1, c2) {
	if (c1.length == 0 || c2.length == 0)
		return [];
	let plead = 0;
	let ploose = 0;

	let leader = c1[0] < c2[0] ? c1 : c2;
	let looser = c1[0] >= c2[0] ? c1 : c2;

	let result = [];
	let last = _.min([c1[c1.length - 1], c2[c2.length - 1]]);
	let next = true;

	while (next) {
		let s1 = leader[plead * 2];
		let e1 = leader[plead * 2 + 1];
		let s2 = looser[ploose * 2];
		let e2 = looser[ploose * 2 + 1];

		if (e1 >= last && e2 >= last) next = false;

		if (s2 < e1) {
			result.push(s2);
		} else {
			plead++;
			s1 = leader[plead * 2];

			if (s1 >= s2) {
				let sw = looser;
				looser = leader;
				leader = sw;

				let swp = ploose;
				ploose = plead;
				plead = swp;
			}
			continue;
		}


		if (e2 > e1) {
			result.push(e1);
			let sw = looser;
			looser = leader;
			leader = sw;

			let swp = ploose;
			ploose = plead;
			plead = swp;
			ploose++;
			continue;
		}

		if (e2 <= e1) {
			result.push(e2);

			ploose++;

			continue;
		}

	}

	return result;
}

function provides(provision, service) {
	if (!provision)
		return false;
	if (provision == '*')
		return true;
	if (provision.constructor == String)
		return provision == service;
	if (provision.constructor == Array)
		return !!~provision.indexOf(service);
}

function debounce(fn, time) {

}

function _planName(agent, organization, d_date_key) {
	return `${agent}-${organization}-plan--${d_date_key}`;
}

class Mosaic {
	// methods
	init(patchwerk, save_cb) {
		this.patchwerk = patchwerk;
		this._save = save_cb;
		this.data = {};
		this.metadata = {};
	}

	observeDays(days, only_service = false) {
		days = _.castArray(days);
		let time = process.hrtime();
		let agents, services, schedules;
		let query = days[0];

		let now = this._now(query);
		let mode = this._modeOption(query);

		//@NOTE only_service is meant only for today prebook, strongly not recommended in any other case
		return (only_service ? Promise.resolve([query.srv]) : this.patchwerk.get('Service', {
				department: query.org_merged.id,
				counter: "*"
			}))
			.then(srvs => {
				services = srvs;
				return this._agents(query, mode);
			})
			.then(res => {
				agents = res;
				// console.log("###############################################################\n", "AGENTS/SERVICES");
				//@NOTE collecting schedule keys from agents
				let sch_obj = {},
					l = agents.length,
					ll;
				while (l--) {
					let sc = agents[l].get("has_schedule");
					sc = sc && sc.prebook || [];
					ll = sc.length;
					while (ll--) {
						if (!sch_obj[sc[ll]])
							sch_obj[sc[ll]] = true;
					}

					sc = agents[l].get("has_schedule");
					sc = _.castArray(sc && sc.resource || []);
					ll = sc.length;
					while (ll--) {
						if (!sch_obj[sc[ll]])
							sch_obj[sc[ll]] = true;
					}
				}

				//@NOTE collecting schedule keys from services
				l = services.length;
				while (l--) {
					let sc = services[l].get("has_schedule");
					sc = sc && sc.prebook || [];
					ll = sc.length;
					while (ll--) {
						if (!sch_obj[sc[ll]])
							sch_obj[sc[ll]] = true;
					}
				}
				// console.log("###############################################################\n", sch_obj);
				let sch_keys = Object.keys(sch_obj);
				return Promise.map(sch_keys, k => this.patchwerk.get('Schedule', {
					key: k
				}));
			})
			.then(sch_data => {
				schedules = sch_data;
				let la = agents.length,
					ls = services.length,
					lsc = schedules.length,
					service_ids = _.map(services, srv => srv.parent.id);
				let amap = {},
					rmap = {},
					srv = {},
					scmap = {},
					pmap = {},
					sch, r_sch,
					l, prov;
				//@NOTE mapping schedules for easier access
				while (lsc--) {
					scmap[schedules[lsc].id] = lsc;
					// console.log("before", schedules[lsc]);
					schedules[lsc] = schedules[lsc].getSource();
					// console.log("after", schedules[lsc]);
					schedules[lsc].has_time_description = _.flatMap(_.castArray(schedules[lsc].has_time_description), 'data.0');
				}

				//@NOTE mapping agents (keymap is faster than _.keyBy)
				while (la--) {
					sch = agents[la].get("has_schedule");
					r_sch = sch && sch.resource;
					sch = sch && sch.prebook;
					if (!sch || !r_sch)
						continue;
					amap[agents[la].id] = [];
					rmap[agents[la].id] = [];
					sch = (sch.constructor == Array ? sch : [sch]);
					l = sch.length;
					while (l--) {
						amap[agents[la].id].push(scmap[sch[l]]);
					}

					r_sch = (r_sch.constructor == Array ? r_sch : [r_sch]);
					l = r_sch.length;
					while (l--) {
						rmap[agents[la].id].push(scmap[r_sch[l]]);
					}

					prov = agents[la].get("provides");
					pmap[agents[la].id] = prov;
				}

				console.log("###############################################################\n", amap);

				//@NOTE days computing
				return Promise.mapSeries(days, day_data => this.patchwerk.get('Ticket', {
							date: day_data.d_date_key,
							department: day_data.org_merged.id,
							counter: "*"
						})
						.then(tickets => {
							if (tickets.length == 1 && !tickets[0]["@id"])
								tickets = [];
							let res = {},
								lines = {},
								active = Object.keys(amap),
								la = active.length,
								line, line_idx,
								r_line, r_line_idx,
								line_sz, gap, optime, curr, nxt, ts, slots_cnt,
								day = day_data.d_date.format('dddd'),
								org_time_description = [0, 86400],
								mask = [day_data.today ? now : 0, 86400];

							//@NOTE forming organization line to apply to general agent lines
							if (day_data.org_merged.has_schedule && day_data.org_merged.has_schedule.prebook) {
								org_time_description = _.find(_.castArray(day_data.org_merged.has_schedule.prebook), s => !!~_.indexOf(s.has_day, day));
								org_time_description = org_time_description && _.flatMap(_.castArray(org_time_description.has_time_description), 'data.0');
							}

							//@NOTE dividing ticks to agent-bound  and any-line
							let ticks_by_agent = _.groupBy(_.filter(tickets, t => (t.get("state") == 'booked' || t.get("state") == 'registered')), t => (t.get(query.agent_type) || 'rest'));
							while (la--) {
								//certain line
								line_idx = _.find(amap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
								line = schedules[line_idx];

								r_line_idx = _.find(rmap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
								r_line = schedules[r_line_idx];

								if (!line || !r_line) continue;
								r_line = r_line.has_time_description.slice();
								r_line = intersect(r_line, mask);
								line = line.has_time_description.slice();
								console.log("line", active[la], line, r_line, day_data.td);
								//@NOTE apply organization line to agent line
								//@NOTE putting bound ticks to lines, ignore unplaced
								if (!!ticks_by_agent[active[la]]) {
									_.map(ticks_by_agent[active[la]], t => {
										insertTick(r_line, t.get("time_description"), t.get("service_count"));
									});
								}
								console.log("befo", r_line);
								//@NOTE putting the rest ticks, ignore unplaced
								if (!!ticks_by_agent.rest) {
									_.map(ticks_by_agent.rest, t => {
										if (!t.placed && provides(pmap[active[la]], t.get("service"))) {
											t.placed = insertTick(r_line, t.get("time_description"), t.get("service_count"));
										}
									});
								}
								console.log("befo", r_line);

								lines[active[la]] = intersect(line, r_line);
							}

							console.log("lines", lines);
							console.log("org_td", org_time_description);

							//@NOTE forming by-service slots on remains of agent lines
							la = active.length;
							while (la--) {
								line = lines[active[la]];
								if (!line)
									continue;

								line = intersect(line, org_time_description);
								line_sz = line.length;

								let sl = services.length,
									service,
									plan_name = _planName(active[la], query.org_merged.id, day_data.d_date_key);
								console.log("plc", active[la], line, line_sz, plan_name);
								for (var ii = 0; ii < sl; ii++) {
									service = services[ii];
									optime = service.get("prebook_operation_time");
									res[service.parent.id] = res[service.parent.id] || [];
									// console.log("provides", active[la], service.parent.id, provides(pmap[active[la]], service.parent.id));
									if (!provides(pmap[active[la]], service.parent.id))
										continue;
									for (var i = 0; i < line_sz; i = i + 2) {
										gap = line[i + 1] - line[i];
										slots_cnt = (gap / optime) | 0;
										curr = line[i];
										for (var j = 0; j < slots_cnt; j++) {
											nxt = curr + optime;
											res[service.parent.id].push({
												time_description: [curr, nxt],
												operator: mode.is_d_mode ? null : active[la],
												destination: mode.is_d_mode ? active[la] : null,
												source: plan_name
											});
											curr = nxt;
										}
									}
								}
							}
							console.log(require("util")
								.inspect(res[query.service], {
									depth: null
								}))

							_.set(this.data, [day_data.org_merged.id, day_data.d_date_key], res);
							_.set(this.metadata, [day_data.org_merged.id, day_data.d_date_key], day_data);

							// return Promise.map(service_ids, s_id => !!res[s_id] && this.save(query.org_merged.id, s_id, day_data.d_date, day_data.d_date_key, res[s_id]), {
							// 	concurrency: 50
							// });
							return res;
						})
					)
					.then(res => {
						let diff = process.hrtime(time);
						console.log('MOSAIC SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
						global.logger && logger.info('MOSAIC SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
						return res;
					});
			})

	}

	clear(section) {
		if (!section)
			return;
		this.data[section] = {};
		this.metadata[section] = {};
	}

	get(path) {
		return _.get(this.data, path);
	}

	save(section) {
		if (!section || !this.data[section])
			return;
		let md, to_save = [];
		for (var i in this.data[section]) {
			md = this.metadata[section][i];
			for (var s_id in this.data[section][i]) {
				to_save.push(this._save(section, s_id, md.d_date, md.d_date_key, this.data[section][i][s_id]));
			}
		}
		return Promise.mapSeries(to_save, t => t)
			.then(res => {
				this.clear(section);
				return res;
			});
	}

	_now(query) {
		return moment.tz(query.org_merged.org_timezone)
			.diff(moment.tz(query.org_merged.org_timezone)
				.startOf('day'), 'seconds') + query.org_merged.prebook_observe_offset;
	}

	_modeOption(query) {
		let is_d_mode = query.agent_type == 'destination';
		let agent_class = is_d_mode ? 'Workstation' : 'Employee';
		return {
			agent_class: agent_class,
			is_d_mode: is_d_mode
		};
	}

	_agents(query, mode) {
		return Promise.map(query.agent_keys.all, k => this.patchwerk.get(mode.agent_class, {
			key: k
		}));
	}

	placementCheck(query, new_ticks) {
		let time = process.hrtime();
		let agents, services, schedules;
		// console.log("###############################################################\n", query.agent_keys, query.d_date_key, only_service);

		//@NOTE agent type is either operator or destination

		let now = this._now(query);
		let mode = this._modeOption(query);

		return this.patchwerk.get('Service', {
				department: query.org_merged.id,
				counter: _.uniq(_.map(new_ticks, t => _.last(t.service.split("-"))))
			})
			.then(srvs => {
				services = srvs;
				return this._agents(query, mode);
			})
			.then(res => {
				agents = res;
				// console.log("###############################################################\n", "AGENTS/SERVICES");
				//@NOTE collecting schedule keys from agents
				let sch_obj = {},
					l = agents.length,
					ll;
				while (l--) {
					let sc = agents[l].get("has_schedule");
					sc = sc && sc.prebook || [];
					ll = sc.length;
					while (ll--) {
						if (!sch_obj[sc[ll]])
							sch_obj[sc[ll]] = true;
					}

					sc = agents[l].get("has_schedule");
					sc = _.castArray(sc && sc.resource || []);
					ll = sc.length;
					while (ll--) {
						if (!sch_obj[sc[ll]])
							sch_obj[sc[ll]] = true;
					}
				}

				//@NOTE collecting schedule keys from services
				l = services.length;
				while (l--) {
					let sc = services[l].get("has_schedule");
					sc = sc && sc.prebook || [];
					ll = sc.length;
					while (ll--) {
						if (!sch_obj[sc[ll]])
							sch_obj[sc[ll]] = true;
					}
				}
				// console.log("###############################################################\n", sch_obj);
				let sch_keys = Object.keys(sch_obj);
				return Promise.map(sch_keys, k => this.patchwerk.get('Schedule', {
					key: k
				}));
			})
			.then(sch_data => {
				schedules = sch_data;
				let la = agents.length,
					ls = services.length,
					lsc = schedules.length,
					service_ids = _.map(services, srv => srv.parent.id);
				let amap = {},
					rmap = {},
					srv = {},
					scmap = {},
					pmap = {},
					sch, r_sch,
					l, prov;
				//@NOTE mapping schedules for easier access
				while (lsc--) {
					scmap[schedules[lsc].id] = lsc;
					// console.log("before", schedules[lsc]);
					schedules[lsc] = schedules[lsc].getSource();
					// console.log("after", schedules[lsc]);
					schedules[lsc].has_time_description = _.flatMap(_.castArray(schedules[lsc].has_time_description), 'data.0');
				}
				// console.log("scmap", scmap);
				// console.log("scmap data", schedules);

				//@NOTE mapping agents (keymap is faster than _.keyBy)
				while (la--) {
					sch = agents[la].get("has_schedule");
					r_sch = sch && sch.resource;
					sch = sch && sch.prebook;
					if (!sch || !r_sch)
						continue;
					amap[agents[la].id] = [];
					rmap[agents[la].id] = [];
					sch = (sch.constructor == Array ? sch : [sch]);
					l = sch.length;
					while (l--) {
						amap[agents[la].id].push(scmap[sch[l]]);
					}

					r_sch = (r_sch.constructor == Array ? r_sch : [r_sch]);
					l = r_sch.length;
					while (l--) {
						rmap[agents[la].id].push(scmap[r_sch[l]]);
					}

					prov = agents[la].get("provides");
					pmap[agents[la].id] = prov;
				}
				console.log("amap", amap);

				let day_data = query;
				return this.patchwerk.get('Ticket', {
						date: day_data.d_date_key,
						department: day_data.org_merged.id,
						counter: "*"
					})
					.then(tickets => {
						if (tickets.length == 1 && !tickets[0]["@id"])
							tickets = [];
						let lines = {},
							active = Object.keys(amap),
							la = active.length,
							line, r_line, line_idx, r_line_idx,
							day = day_data.d_date.format('dddd'),
							org_time_description = [0, 86400],
							mask = [day_data.today ? now : 0, 86400];

						//@NOTE forming organization line to apply to general agent lines
						if (day_data.org_merged.has_schedule && day_data.org_merged.has_schedule.prebook) {
							org_time_description = _.find(_.castArray(day_data.org_merged.has_schedule.prebook), s => !!~_.indexOf(s.has_day, day));
							org_time_description = org_time_description && _.flatMap(_.castArray(org_time_description.has_time_description), 'data.0');
						}

						//@NOTE dividing ticks to agent-bound  and any-line
						let ticks_by_agent = _.groupBy(_.filter(tickets, t => (t.get("state") == 'booked' || t.get("state") == 'registered')), t => (t.get(query.agent_type) || 'rest'));
						while (la--) {
							//certain line
							line_idx = _.find(amap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
							line = schedules[line_idx];

							r_line_idx = _.find(rmap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
							r_line = schedules[r_line_idx];

							if (!line || !r_line) continue;
							r_line = r_line.has_time_description.slice();
							r_line = intersect(r_line, mask);
							line = line.has_time_description.slice();
							console.log("line", active[la], line, r_line, day_data.td);

							//@NOTE apply organization line to agent line
							//@NOTE putting bound ticks to lines, ignore unplaced
							if (!!ticks_by_agent[active[la]]) {
								_.map(ticks_by_agent[active[la]], t => {
									insertTick(r_line, t.get("time_description"), t.get("service_count"));
								});
							}
							// console.log("befo", line);
							//@NOTE putting the rest ticks, ignore unplaced
							if (!!ticks_by_agent.rest) {
								_.map(ticks_by_agent.rest, t => {
									if (!t.placed && provides(pmap[active[la]], t.get("service"))) {
										t.placed = insertTick(r_line, t.get("time_description"), t.get("service_count"));
									}
								});
							}
							lines[active[la]] = intersect(line, r_line);
						}

						console.log("lines", lines);
						console.log("org_td", org_time_description);
						// console.log("new tick by agent", new_ticks);

						la = active.length;
						let new_ticks_by_agent = _.groupBy(_.filter(new_ticks, t => (t.state == 'booked' || t.state == 'registered')), t => (t[query.agent_type] || 'rest'));
						while (la--) {
							console.log(active[la], lines[active[la]]);
							line = lines[active[la]];
							if (!line)
								continue;
							line = intersect(line, org_time_description);

							if (!!new_ticks_by_agent[active[la]]) {
								_.map(new_ticks_by_agent[active[la]], t => {
									insertTick(line, t.time_description, t.service_count);
								});
							}
							// console.log("befo", line);
							//@NOTE putting the rest ticks, ignore unplaced
							if (!!new_ticks_by_agent.rest) {
								_.map(new_ticks_by_agent.rest, t => {
									if (!t.placed && provides(pmap[active[la]], t.service)) {
										t.placed = insertTick(line, t.time_description, t.service_count);
									}
								});
							}
						}

						let lt = new_ticks.length,
							placed = [],
							lost = [];
						while (lt--) {
							if (new_ticks[lt].placed) {
								placed.push(new_ticks[lt]);
								_.unset(new_ticks[lt], "placed");
							} else {
								lost.push(new_ticks[lt]);
							}
						}
						let diff = process.hrtime(time);
						console.log('OBSERVED MOSAIC SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
						return {
							success: placed.length == new_ticks.length,
							placed: placed,
							lost: lost
						};
					});
			})

	}


}


let instance = new Mosaic();
module.exports = instance;