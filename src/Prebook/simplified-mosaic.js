'use strict';

function insertTick(plan, tick) {
	let l = plan.length,
		success = false;
	for (var i = 1; i < l; i = i + 2) {
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
					sch, l;
				while (lsc--) {
					scmap[schedules[lsc].id] = lsc;
					// console.log("before", schedules[lsc]);
					schedules[lsc] = schedules[lsc].getSource();
					// console.log("after", schedules[lsc]);
					schedules[lsc].has_time_description = schedules[lsc].has_time_description.data[0];
				}
				while (la--) {
					sch = agents[la].get("has_schedule");
					sch = sch && sch.prebook;
					if (!sch)
						continue;
					amap[agents[la].id] = [];
					sch = (sch.constructor === Array ? sch : [sch]);
					l = sch.length;
					while (l--) {
						amap[agents[la].id].push(scmap[sch[l]]);
					}
				}
				console.log("###############################################################\n", amap);

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
							let ticks_by_agent = _.groupBy(tickets, t => (t.get(query.agent_type) || 'rest'));
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
										if (!t.placed) {
											t.placed = insertTick(line, t.get("time_description"));
										}
									});
								}
								line_sz = line.length;
								console.log("plc", line, line_sz);
								_.forEach(services, service => {
									optime = service.get("prebook_operation_time");
									res[service.parent.id] = res[service.parent.id] || [];
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
												source: true
											});
											curr = nxt;
										}
									}
								})

								// console.log(active[la], line);
							}
							// console.log(require("util")
							// 	.inspect(res, {
							// 		depth: null
							// 	}))
							return Promise.mapSeries(service_ids, s_id => !!res[s_id] && this.save(query.org_merged.id, s_id, day_data.d_date, day_data.d_date_key, res[s_id]));
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