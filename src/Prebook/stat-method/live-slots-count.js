module.exports = {
	name: 'live_slots_count',
	compute: function (datapart) {
		let val = (datapart.forced_max_live_slots_count - datapart.forced_live_slots_count);
		return _.isNumber(datapart.forced_max_live_slots_count) ? val : _.clamp(datapart.live_slots_count - _.floor(1800 / datapart.live_expiry), 0, Infinity);
	}
};