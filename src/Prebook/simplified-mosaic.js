'use strict';

function _isArray(val) {
	return val && (val.constructor == Array);
}

function statLine(initial, filled) {
	let baseline = initial;
	let length = 0,
		a_length = 0,
		chunk, max_solid = 0,
		l = baseline.length;
	for (var i = 1; i < l; i = i + 2) {
		chunk = baseline[i] - baseline[i - 1];
		max_solid = max_solid > chunk ? max_solid : chunk;
		length += chunk;
	}
	baseline = filled;
	for (var i = 1; i < l; i = i + 2) {
		chunk = baseline[i] - baseline[i - 1];
		max_solid = max_solid > chunk ? max_solid : chunk;
	}
	return {
		max_available: length,
		available: a_length,
		max_solid: max_solid
	};
}

function statTickets(ticks) {
	let l = ticks.length,
		reserved_live = {},
		reserved_prebook = {},
		td, srv;
	while (l--) {
		srv = ticks[l].get("service");
		td = ticks[l].get("time_description");
		if (ticks[l].get("booking_method") == "prebook") {
			reserved_prebook[srv] = reserved_prebook[srv] || 0;
			reserved_prebook[srv] += (td.constructor == Array ? td[1] - td[0] : td);
		}
		if (ticks[l].get("booking_method") == "live") {
			reserved_live[srv] = reserved_live[srv] || 0;
			reserved_live[srv] += (td.constructor == Array ? td[1] - td[0] : td);
		}
	}
	return {
		reserved_prebook: reserved_prebook,
		reserved_live: reserved_live
	};
}

function insertTick(plan, tick_td, sc = 1) {
	let l = plan.length,
		success = false,
		tick;
	// console.log("INSERTING TICK", plan, tick_td);
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
	return success && tick;
}

function markReserved(plan, tick) {
	let chunks = intersect(plan, tick),
		l = chunks.length;
	for (var i = 1; i < l; i = i + 2) {
		insertTick(plan, [chunks[i - 1], chunks[i]]);
	}
}


function intersect(c1, c2) {
	let plead = 0;
	let ploose = 0;

	let leader = c1[0] < c2[0] ? c1 : c2;
	let looser = c1[0] >= c2[0] ? c1 : c2;

	let result = [];
	if (c1.length == 0 || c2.length == 0) {
		return result;
	}
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

function canPlace(agent, tick, initial_query, raw = false) {
	return provides(agent.get("provides"), (raw ? tick.service : tick.get("service")));
}

function agentState(agent, query, method) {
	return !query.today || !!~query.agent_keys.active.indexOf(agent.id);
}

function bookingMethod(agent_method, tick_method) {
	if (!agent_method)
		return true;
	if (agent_method == '*')
		return true;
	if (agent_method.constructor == String)
		return agent_method == tick_method;
	if (provision.constructor == Array)
		return !!~agent_method.indexOf(tick_method);
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
		this.save = save_cb;
	}


	checkPlacement(query, new_ticks) {
		let time = process.hrtime();
		let agents, services, schedules;
		let new_tickets = _.castArray(new_ticks);
		// console.log("###############################################################\n", query.agent_keys, query.d_date_key, only_service);

		//@NOTE agent type is either operator or destination

		let now = this._now(query);
		let mode = this._modeOption(query);
		let active_states = ['registered', 'booked', 'called', 'postponed'];

		return this.patchwerk.get('Service', {
				department: query.org_merged.id,
				counter: _.map(new_tickets, in_tick => _.last(in_tick.service.split("-")))
			})
			.then(srvs => {
				services = _.castArray(srvs);
				return this._agents(query, mode);
			})
			.then(res => {
				agents = res;
				// console.log("###############################################################\n", "AGENTS/SERVICES");
				//@NOTE collecting schedule keys from agents
				let sch_obj = {},
					l = agents.length,
					ll, sca, sc;
				while (l--) {
					sc = agents[l].get("has_schedule");
					sca = _.castArray(sc && sc.prebook || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
					}

					sca = _.castArray(sc && sc.resource || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
					}

					sca = _.castArray(sc && sc.live || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
					}
				}

				//@NOTE collecting schedule keys from services
				l = services.length;
				while (l--) {
					sc = services[l].get("has_schedule");
					sca = _.castArray(sc && sc.prebook || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
					}

					sca = _.castArray(sc && sc.live || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
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
					lmap = {},
					p_srv = {},
					l_srv = {},
					scmap = {},
					pmap = {},
					sch, r_sch, l_sch,
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
					l_sch = sch && sch.live;
					sch = sch && sch.prebook;
					if (!r_sch)
						continue;
					amap[agents[la].id] = [];
					rmap[agents[la].id] = [];
					lmap[agents[la].id] = [];
					if (sch) {
						sch = (sch.constructor == Array ? sch : [sch]);
						l = sch.length;
						while (l--) {
							amap[agents[la].id].push(scmap[sch[l]]);
						}
					}

					r_sch = (r_sch.constructor == Array ? r_sch : [r_sch]);
					l = r_sch.length;
					while (l--) {
						rmap[agents[la].id].push(scmap[r_sch[l]]);
					}

					if (l_sch) {
						l_sch = (l_sch.constructor == Array ? l_sch : [l_sch]);
						l = l_sch.length;
						while (l--) {
							lmap[agents[la].id].push(scmap[l_sch[l]]);
						}
					}

					pmap[agents[la].id] = agents[la];
				}
				// console.log("amap", amap);
				la = services.length;
				while (la--) {
					sch = services[la].get("has_schedule");
					l_sch = sch && sch.live;
					r_sch = sch && sch.prebook;
					p_srv[services[la].parent.id] = [];
					l_srv[services[la].parent.id] = [];
					if (r_sch) {
						r_sch = (r_sch.constructor == Array ? r_sch : [r_sch]);
						l = r_sch.length;
						while (l--) {
							p_srv[services[la].parent.id].push(scmap[r_sch[l]]);
						}
					}

					if (l_sch) {
						l_sch = (l_sch.constructor == Array ? l_sch : [l_sch]);
						l = l_sch.length;
						while (l--) {
							l_srv[services[la].parent.id].push(scmap[l_sch[l]]);
						}
					}
				}

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
							// l_slots = {},
							p_slots = {},
							pline_stats = {},
							p_stats = {},
							active = Object.keys(rmap),
							la = active.length,
							line, r_line, line_idx, r_line_idx,
							line_sz, gap, optime, curr, nxt, ts, slots_cnt,
							day = day_data.d_date.format('dddd'),
							org_time_description = [0, 86400],
							mask = [now, 86400],
							pb_mask = mask.slice();
						pb_mask[0] = pb_mask[0] + (day_data.org_merged.prebook_observe_offset || 0);

						//@NOTE forming organization line to apply to general agent lines
						if (day_data.org_merged.has_schedule && day_data.org_merged.has_schedule.prebook) {
							org_time_description = _.find(_.castArray(day_data.org_merged.has_schedule.prebook), s => !!~_.indexOf(s.has_day, day));
							org_time_description = org_time_description && _.flatMap(_.castArray(org_time_description.has_time_description), 'data.0');
						}

						//@NOTE dividing ticks to agent-bound  and any-line
						let ticks_by_agent = _.groupBy(_.filter(tickets, t => !!~active_states.indexOf(t.get("state"))), t => (t.get(query.agent_type) || 'rest'));
						while (la--) {
							//certain line
							line_idx = _.find(amap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
							line = schedules[line_idx];
							lines[active[la]] = lines[active[la]] || {};
							if (line) {
								line = line.has_time_description.slice();
								lines[active[la]].prebook = line;
							}

							line_idx = _.find(lmap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
							line = schedules[line_idx];
							if (line) {
								line = line.has_time_description.slice();
								lines[active[la]].live = line;
							}

						}
						console.log("lines pre", lines);

						la = active.length;
						while (la--) {
							r_line = lines[active[la]].prebook && lines[active[la]].prebook.slice();
							line = lines[active[la]].live && intersect(lines[active[la]].live, mask);
							console.log(active[la], "p", r_line, "L", line);
							if (!!ticks_by_agent[active[la]]) {
								_.map(ticks_by_agent[active[la]], t => {
									if (r_line && (t.get("booking_method") == "prebook")) {
										t.placed = insertTick(r_line, t.get("time_description"));
										t.placed && !!line && markReserved(line, t.placed);
									}
									if (line && (t.get("booking_method") == "live")) {
										t.placed = insertTick(line, t.get("time_description"), t.get("service_count"));
										t.placed && !!r_line && markReserved(r_line, t.placed);
									}
								});
							}
							if (!!ticks_by_agent.rest) {
								_.map(ticks_by_agent.rest, t => {
									// console.log(">>", t.id, t.placed, t.booking_method, canPlace(pmap[active[la]], t, query));
									if (!t.placed && canPlace(pmap[active[la]], t, query)) {
										if (r_line && (t.get("booking_method") == "prebook")) {
											t.placed = insertTick(r_line, t.get("time_description"));
											t.placed && !!line && markReserved(line, t.placed);
										}
										if (line && (t.get("booking_method") == "live")) {
											t.placed = insertTick(line, t.get("time_description"), t.get("service_count"));
											t.placed && !!r_line && markReserved(r_line, t.placed);
										}
									}
								});
							}
							if (r_line) {
								pline_stats[active[la]] = statLine(lines[active[la]].prebook, r_line);
							}
							line && (lines[active[la]].live = line);
							r_line && (lines[active[la]].prebook = intersect(r_line, mask));
						}


						console.log("lines", lines);
						console.log("org_td", org_time_description);
						// console.log("new tick by agent", new_ticks);
						let ticks_reserved = (statTickets(tickets))
							.reserved_prebook;

						let a_line, a_line_sz,
							s_lines = {};
						la = services.length;
						while (la--) {
							line_idx = _.find(p_srv[services[la].parent.id], s => !!~_.indexOf(schedules[s].has_day, day));
							line = schedules[line_idx];
							s_lines[services[la].parent.id] = s_lines[services[la].parent.id] || {};
							if (line) {
								line = line.has_time_description.slice();
								s_lines[services[la].parent.id].prebook = line;
							}
						}
						// console.log("slines", s_lines);
						la = active.length;
						while (la--) {
							// console.log(active[la], lines[active[la]]);
							a_line = lines[active[la]].prebook;
							if (!a_line)
								continue;
							// line = intersect(line, org_time_description);
							a_line_sz = a_line.length;
							// console.log("appliable for prebook", active[la], lines[active[la]].prebook);
							let sl = services.length,
								service;
							for (var ii = 0; ii < sl; ii++) {
								service = services[ii];
								line = s_lines[service.parent.id].prebook || [];
								line = intersect(a_line, line);
								line_sz = line.length;
								// console.log("line", line, service.parent.id, "prebook");
								//stats
								p_stats[service.parent.id] = p_stats[service.parent.id] || {
									part: _.clamp(service.get("prebook_today_percentage") || 0, 0, 100) / 100,
									max_available: 0,
									reserved: ticks_reserved[service.parent.id] || 0
								};

								if (!provides(pmap[active[la]].get("provides"), service.parent.id))
									continue;

								p_stats[service.parent.id].max_available += _.get(pline_stats, [active[la], 'max_available'], 0);
							}

						}


						let result = {
							placed: [],
							lost: [],
							success: false
						};
						let part, real_part, tl = new_tickets.length,
							t_srv;
						for (var ii = 0; ii < tl; ii++) {
							t_srv = new_tickets[ii].service;
							part = p_stats[t_srv].part;
							real_part = p_stats[t_srv].reserved / p_stats[t_srv].max_available;
							new_tickets[ii].quota_pass = (part > real_part);
							console.log(new_tickets[ii]);
						}
						let new_tickets_by_agent = _.groupBy(new_tickets, t => (t[query.agent_type] || 'rest'));

						console.log("<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>", tl, new_tickets_by_agent, p_stats);


						la = active.length;
						while (la--) {
							r_line = lines[active[la]].prebook;
							line = lines[active[la]].live;

							if (!!new_tickets_by_agent[active[la]]) {
								_.map(new_tickets_by_agent[active[la]], t => {
									if (!t.quota_pass || !!t.placed)
										return;
									if (r_line && (t.booking_method == "prebook")) {
										t.placed = insertTick(r_line, t.time_description);
										t.placed && !!line && markReserved(line, t.placed);
									}
									if (line && (t.booking_method == "live")) {
										t.placed = insertTick(line, t.time_description, t.service_count);
										t.placed && !!r_line && markReserved(r_line, t.placed);
									}
								});
							}
							if (!!new_tickets_by_agent.rest) {
								_.map(new_tickets_by_agent.rest, t => {
									console.log(">>", t.id, t.placed, t.booking_method, canPlace(pmap[active[la]], t, query, true), r_line);
									if (t.quota_pass && !t.placed && canPlace(pmap[active[la]], t, query, true)) {

										if (r_line && (t.booking_method == "prebook")) {
											t.placed = insertTick(r_line, t.time_description);
											t.placed && !!line && markReserved(line, t.placed);
										}
										if (line && (t.booking_method == "live")) {
											t.placed = insertTick(line, t.time_description, t.service_count);
											t.placed && !!r_line && markReserved(r_line, t.placed);
										}
									}
								});
							}
						}
						let t;
						for (var ii = 0; ii < tl; ii++) {
							t = new_tickets[ii];
							if (!t.quota_pass || !t.placed)
								result.lost.push(t);
							else
								result.placed.push(t);
						}
						result.success = (result.placed.length == new_tickets.length);
						console.log(result);
						let diff = process.hrtime(time);
						console.log('OBSERVED MOSAIC SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
						return result;
					});
			})

	}


	daySlots(query, all = false) {
		let time = process.hrtime();
		let agents, services, schedules;
		console.log("###############################################################\n", query);
		//@NOTE agent type is either operator or destination

		let now = this._now(query);
		let mode = this._modeOption(query);
		let active_states = ['registered', 'booked', 'called', 'postponed'];

		return this.patchwerk.get('Service', {
				department: query.org_merged.id,
				counter: all ? "*" : _.last(query.service.split("-"))
			})
			.then(srvs => {
				services = _.castArray(srvs);
				return this._agents(query, mode);
			})
			.then(res => {
				agents = res;
				// console.log("###############################################################\n", "AGENTS/SERVICES");
				//@NOTE collecting schedule keys from agents
				let sch_obj = {},
					l = agents.length,
					ll, sca, sc;
				while (l--) {
					sc = agents[l].get("has_schedule");
					sca = _.castArray(sc && sc.prebook || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
					}

					sca = _.castArray(sc && sc.resource || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
					}

					sca = _.castArray(sc && sc.live || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
					}
				}

				//@NOTE collecting schedule keys from services
				l = services.length;
				while (l--) {
					sc = services[l].get("has_schedule");
					sca = _.castArray(sc && sc.prebook || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
					}

					sca = _.castArray(sc && sc.live || []);
					ll = sca.length;
					while (ll--) {
						if (!sch_obj[sca[ll]])
							sch_obj[sca[ll]] = true;
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
					lmap = {},
					p_srv = {},
					l_srv = {},
					scmap = {},
					pmap = {},
					sch, r_sch, l_sch,
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
					l_sch = sch && sch.live;
					sch = sch && sch.prebook;
					if (!r_sch)
						continue;
					amap[agents[la].id] = [];
					rmap[agents[la].id] = [];
					lmap[agents[la].id] = [];
					if (sch) {
						sch = (sch.constructor == Array ? sch : [sch]);
						l = sch.length;
						while (l--) {
							amap[agents[la].id].push(scmap[sch[l]]);
						}
					}

					r_sch = (r_sch.constructor == Array ? r_sch : [r_sch]);
					l = r_sch.length;
					while (l--) {
						rmap[agents[la].id].push(scmap[r_sch[l]]);
					}

					if (l_sch) {
						l_sch = (l_sch.constructor == Array ? l_sch : [l_sch]);
						l = l_sch.length;
						while (l--) {
							lmap[agents[la].id].push(scmap[l_sch[l]]);
						}
					}

					pmap[agents[la].id] = agents[la];
				}
				// console.log("amap", amap);
				la = services.length;
				while (la--) {
					sch = services[la].get("has_schedule");
					l_sch = sch && sch.live;
					r_sch = sch && sch.prebook;
					p_srv[services[la].parent.id] = [];
					l_srv[services[la].parent.id] = [];
					if (r_sch) {
						r_sch = (r_sch.constructor == Array ? r_sch : [r_sch]);
						l = r_sch.length;
						while (l--) {
							p_srv[services[la].parent.id].push(scmap[r_sch[l]]);
						}
					}

					if (l_sch) {
						l_sch = (l_sch.constructor == Array ? l_sch : [l_sch]);
						l = l_sch.length;
						while (l--) {
							l_srv[services[la].parent.id].push(scmap[l_sch[l]]);
						}
					}
				}

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
							// l_slots = {},
							p_slots = {},
							pline_stats = {},
							p_stats = {},
							active = Object.keys(rmap),
							la = active.length,
							line, r_line, line_idx, r_line_idx,
							line_sz, gap, optime, curr, nxt, ts, slots_cnt,
							day = day_data.d_date.format('dddd'),
							org_time_description = [0, 86400],
							mask = [now, 86400],
							pb_mask = mask.slice();
						pb_mask[0] = pb_mask[0] + (day_data.org_merged.prebook_observe_offset || 0);
						console.log("MASKS", mask, pb_mask, day_data);

						//@NOTE forming organization line to apply to general agent lines
						if (day_data.org_merged.has_schedule && day_data.org_merged.has_schedule.prebook) {
							org_time_description = _.find(_.castArray(day_data.org_merged.has_schedule.prebook), s => !!~_.indexOf(s.has_day, day));
							org_time_description = org_time_description && _.flatMap(_.castArray(org_time_description.has_time_description), 'data.0');
						}

						//@NOTE dividing ticks to agent-bound  and any-line
						let ticks_by_agent = _.groupBy(_.filter(tickets, t => !!~active_states.indexOf(t.get("state"))), t => (t.get(query.agent_type) || 'rest'));
						while (la--) {
							//certain line
							line_idx = _.find(amap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
							line = schedules[line_idx];
							lines[active[la]] = lines[active[la]] || {};
							if (line) {
								line = line.has_time_description.slice();
								lines[active[la]].prebook = line;
							}

							line_idx = _.find(lmap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
							line = schedules[line_idx];
							if (line) {
								line = line.has_time_description.slice();
								lines[active[la]].live = line;
							}

						}
						console.log("lines pre", lines);

						la = active.length;
						while (la--) {
							r_line = lines[active[la]].prebook && lines[active[la]].prebook.slice();
							line = lines[active[la]].live && intersect(lines[active[la]].live, mask);
							console.log(active[la], "p", r_line, "L", line);
							if (!!ticks_by_agent[active[la]]) {
								_.map(ticks_by_agent[active[la]], t => {
									if (r_line && (t.get("booking_method") == "prebook")) {
										t.placed = insertTick(r_line, t.get("time_description"));
										t.placed && !!line && markReserved(line, t.placed);
									}
									if (line && (t.get("booking_method") == "live")) {
										t.placed = insertTick(line, t.get("time_description"), t.get("service_count"));
										t.placed && !!r_line && markReserved(r_line, t.placed);
									}
								});
							}
							if (!!ticks_by_agent.rest) {
								_.map(ticks_by_agent.rest, t => {
									// console.log(">>", t.id, t.placed, t.booking_method, canPlace(pmap[active[la]], t, query));
									if (!t.placed && canPlace(pmap[active[la]], t, query)) {
										if (r_line && (t.get("booking_method") == "prebook")) {
											t.placed = insertTick(r_line, t.get("time_description"));
											t.placed && !!line && markReserved(line, t.placed);
										}
										if (line && (t.get("booking_method") == "live")) {
											t.placed = insertTick(line, t.get("time_description"), t.get("service_count"));
											t.placed && !!r_line && markReserved(r_line, t.placed);
										}
									}
								});
							}
							if (r_line) {
								pline_stats[active[la]] = statLine(lines[active[la]].prebook, r_line);
							}
							console.log("lines after", "p", r_line, "L", line);
							line && (lines[active[la]].live = line);
							r_line && (lines[active[la]].prebook = intersect(r_line, pb_mask));
							console.log("lines after II", lines[active[la]]);
						}


						console.log("lines", lines);
						console.log("org_td", org_time_description);
						// console.log("new tick by agent", new_ticks);
						let ticks_reserved = (statTickets(tickets))
							.reserved_prebook;

						let a_line, a_line_sz,
							s_lines = {};
						la = services.length;
						while (la--) {
							line_idx = _.find(p_srv[services[la].parent.id], s => !!~_.indexOf(schedules[s].has_day, day));
							line = schedules[line_idx];
							s_lines[services[la].parent.id] = s_lines[services[la].parent.id] || {};
							if (line) {
								line = line.has_time_description.slice();
								s_lines[services[la].parent.id].prebook = line;
							}

						}
						// console.log("slines", s_lines);
						la = active.length;
						while (la--) {
							// console.log(active[la], lines[active[la]]);
							a_line = lines[active[la]].prebook;
							if (!a_line)
								continue;
							// line = intersect(line, org_time_description);
							a_line_sz = a_line.length;
							// console.log("appliable for prebook", active[la], lines[active[la]].prebook);

							let sl = services.length,
								plan_name = _planName(active[la], query.org_merged.id, day_data.d_date_key),
								service;
							for (var ii = 0; ii < sl; ii++) {
								service = services[ii];
								line = s_lines[service.parent.id].prebook || [];
								line = intersect(a_line, line);
								line_sz = line.length;
								// console.log("line", line, service.parent.id, "prebook");
								//stats
								p_stats[service.parent.id] = p_stats[service.parent.id] || {
									part: _.clamp(service.get("prebook_today_percentage") || 0, 0, 100) / 100,
									max_available: 0,
									reserved: ticks_reserved[service.parent.id] || 0
								};
								if (!provides(pmap[active[la]].get("provides"), service.parent.id))
									continue;
								p_stats[service.parent.id].max_available += _.get(pline_stats, [active[la], 'max_available'], 0);
								//slots
								optime = service.get("prebook_operation_time");
								p_slots[service.parent.id] = p_slots[service.parent.id] || [];
								// console.log("provides", active[la], service.parent.id, provides(pmap[active[la]], service.parent.id));
								let part = p_stats[service.parent.id].part;
								let real_part = p_stats[service.parent.id].reserved / p_stats[service.parent.id].max_available;
								console.log("PARTS", part, real_part, part < real_part);
								if (part <= real_part)
									continue;

								for (var i = 0; i < line_sz; i = i + 2) {
									gap = line[i + 1] - line[i];
									slots_cnt = (gap / optime) | 0;
									curr = line[i];
									// console.log(service.parent.id, i, gap, line[i + 1], line[i], line_sz);
									for (var j = 0; j < slots_cnt; j++) {
										nxt = curr + optime;
										// console.log(service.parent.id, j, curr, nxt);
										p_slots[service.parent.id].push({
											time_description: [curr, nxt],
											operator: mode.is_d_mode ? null : active[la],
											destination: mode.is_d_mode ? active[la] : null,
											source: plan_name
										});
										curr = nxt;
									}
								}
								// console.log("slots for prebook", service.parent.id, p_slots[service.parent.id].length);
							}
						}

						let diff = process.hrtime(time);
						console.log('OBSERVED MOSAIC SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
						return {
							prebook: all ? p_slots : p_slots[query.service],
							prebook_stats: p_stats
						};
					});
			})

	}


	observeDays(days) {
		let time = process.hrtime();
		let agents, services, schedules;
		let query = days[0],
			is_d_mode = query.agent_type == 'destination';
		console.log("######################QUERY##########################\n", query);
		let agent_class = is_d_mode ? 'Workstation' : 'Employee';
		return this.patchwerk.get('Service', {
				department: query.org_merged.id,
				counter: "*"
			})
			.then(srvs => {
				services = srvs;
				return Promise.map(query.agent_keys.all, k => this.patchwerk.get(agent_class, {
					key: k
				}));
			})
			.then(res => {
				agents = res;
				// console.log("###############################################################\n", "AGENTS/SERVICES");
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
				}
				console.log("###############################################################\n", sch_obj);
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
					srv = {},
					scmap = {},
					pmap = {},
					sch, l, prov;
				while (lsc--) {
					scmap[schedules[lsc].id] = lsc;
					// console.log("before", schedules[lsc]);
					schedules[lsc] = schedules[lsc].getSource();
					// console.log("after", schedules[lsc]);
					schedules[lsc].has_time_description = _.flatMap(_.castArray(schedules[lsc].has_time_description), 'data.0');
				}
				console.log("SCHEDULES", schedules);
				while (la--) {
					sch = agents[la].get("has_schedule");
					sch = sch && sch.prebook;
					if (!sch)
						continue;
					amap[agents[la].id] = [];
					sch = (sch.constructor == Array ? sch : [sch]);
					l = sch.length;
					while (l--) {
						amap[agents[la].id].push(scmap[sch[l]]);
					}

					prov = agents[la].get("provides");
					pmap[agents[la].id] = prov;
				}
				console.log("AMAP\n", amap);

				return Promise.mapSeries(days, day_data => this.patchwerk.get('Ticket', {
							date: day_data.d_date_key,
							department: day_data.org_merged.id,
							counter: "*"
						})
						.then(tickets => {
							if (tickets.length == 1 && !tickets[0]["@id"])
								tickets = [];
							let res = {},
								active = Object.keys(amap),
								la = active.length,
								line, line_idx, line_sz, gap, optime, curr, nxt, ts, slots_cnt,
								day = day_data.d_date.format('dddd');
							let ticks_by_agent = _.groupBy(_.filter(tickets, t => (t.get("state") == 'booked' || t.get("state") == 'registered')), t => (t.get(query.agent_type) || 'rest'));
							while (la--) {
								//certain line
								line_idx = _.find(amap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
								line = schedules[line_idx];
								// console.log("try", line, line_idx, amap[active[la]], active[la], day, schedules);
								if (!line) continue;
								line = line.has_time_description.slice();
								if (!!ticks_by_agent[active[la]]) {
									_.map(ticks_by_agent[active[la]], t => {
										insertTick(line, t.get("time_description"));
									});
								}
								// console.log("befo", line);
								if (!!ticks_by_agent.rest) {
									_.map(ticks_by_agent.rest, t => {
										if (!t.placed && provides(pmap[active[la]], t.get("service"))) {
											t.placed = insertTick(line, t.get("time_description"));
										}
									});
								}
								line_sz = line.length;
								console.log("plc", line, line_sz);

								let sl = services.length,
									plan_name = _planName(active[la], query.org_merged.id, day_data.d_date_key),
									service;
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
										// console.log(service.parent.id, i, gap, line[i + 1], line[i], line_sz);
										for (var j = 0; j < slots_cnt; j++) {
											nxt = curr + optime;
											// console.log(service.parent.id, j, curr, nxt);
											res[service.parent.id].push({
												time_description: [curr, nxt],
												operator: is_d_mode ? null : active[la],
												destination: is_d_mode ? active[la] : null,
												source: plan_name
											});
											curr = nxt;
										}
									}
								}

								// console.log(active[la], line);
							}
							// console.log("RESULTS", require("util")
							// 	.inspect(res[query.service], {
							// 		depth: null
							// 	}))

							console.log("SAVING MOSAIC");
							return Promise.map(service_ids, s_id => this.save(query.org_merged.id, s_id, day_data.d_date, day_data.d_date_key, res[s_id] || []), {
								concurrency: 50
							});
						})
						// .then(res => console.log(day_data.d_date_key))
					)
					.then(res => {
						let diff = process.hrtime(time);
						console.log('MOSAIC SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
						global.logger && logger.info('MOSAIC SLOTS IN %d seconds', diff[0] + diff[1] / 1e9);
					});
			})

	}


	_now(query) {
		return moment.tz(query.org_merged.org_timezone)
			.diff(moment.tz(query.org_merged.org_timezone)
				.startOf('day'), 'seconds');
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

}


let instance = new Mosaic();
module.exports = instance;