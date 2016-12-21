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
		this.save = save_cb;
	}

	observeDays(days) {
		let time = process.hrtime();
		let agents, services, schedules;
		let query = days[0],
			is_d_mode = query.agent_type == 'destination';
		// console.log("###############################################################\n", query);
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
					srv = {},
					scmap = {},
					pmap = {},
					sch, l, prov;
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
					amap[agents[la].id + "-resource"] = [];
					sch = (sch.constructor == Array ? sch : [sch]);
					l = sch.length;
					while (l--) {
						amap[agents[la].id].push(scmap[sch[l]]);
					}

					l = r_sch.length;
					while (l--) {
						amap[agents[la].id + "-resource"].push(scmap[r_sch[l]]);
					}

					prov = agents[la].get("provides");
					pmap[agents[la].id] = prov;
				}

				// console.log("###############################################################\n", amap);

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
								active = Object.keys(amap),
								la = active.length,
								line, line_idx, line_sz, gap, optime, curr, nxt, ts, slots_cnt,
								day = day_data.d_date.format('dddd');
							let ticks_by_agent = _.groupBy(_.filter(tickets, t => (t.get("state") == 'booked' || t.get("state") == 'registered')), t => (t.get(query.agent_type) || 'rest'));
							while (la--) {
								//certain line
								line_idx = _.find(amap[active[la]], s => !!~_.indexOf(schedules[s].has_day, day));
								line = schedules[line_idx];
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
												operator: is_d_mode ? null : agents[la].id,
												destination: is_d_mode ? agents[la].id : null,
												source: _planName(agents[la].id, query.org_merged.id, day_data.d_date_key)
											});
											curr = nxt;
										}
									}
								}

								// console.log(active[la], line);
							}
							// console.log(require("util")
							// 	.inspect(res, {
							// 		depth: null
							// 	}))

							console.log("SAVING MOSAIC");
							return Promise.map(service_ids, s_id => !!res[s_id] && this.save(query.org_merged.id, s_id, day_data.d_date, day_data.d_date_key, res[s_id]), {
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


}


let instance = new Mosaic();
module.exports = instance;