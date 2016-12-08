'use strict';

module.exports = {
	validate: function validate(params, actual, source_info) {
		let method = source_info.booking_method;
		let sc = 0,
			l = source_info.service_count.length;
		while (l--) {
			sc += source_info.service_count[l];
		}
		let act = actual && actual.value || {},
			ts_key = `timestamp_${method}`,
			cnt_key = `count_${method}`;

		!_.isNumber(act[cnt_key]) && (act[cnt_key] = 0);
		!_.isNumber(act[ts_key]) && (act[ts_key] = 0);

		// console.log((Date.now() - act[ts_key]) / 60000, params.interval * 1440, "DATE<<<<<<<<<<<<<<<<<<");
		if (Date.now() - params.interval * 86400000 > act[ts_key]) return true;
		if (act[cnt_key] + sc <= params[cnt_key]) return true;
		return false;
	},
	update: function update(params, actual, source_info) {
		let method = source_info.booking_method;
		let sc = 0,
			l = source_info.service_count.length;
		while (l--) {
			sc += source_info.service_count[l];
		}
		let act = actual && actual.value || {},
			ts_key = `timestamp_${method}`,
			cnt_key = `count_${method}`;

		!_.isNumber(act[cnt_key]) && (act[cnt_key] = 0);
		!_.isNumber(act[ts_key]) && (act[ts_key] = 0);
		// console.log("AAAAAAAAAAAAA", act, Date.now() - params.interval * 86400000 > act[ts_key]);
		if (Date.now() - params.interval * 86400000 > act[ts_key]) {
			act[cnt_key] = 0;
			act[ts_key] = Date.now();
		}
		act[cnt_key] += sc;

		return act;
	}
};