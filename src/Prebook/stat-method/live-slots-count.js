module.exports = {
	name: 'live_slots_count',
	compute: function (datapart) {
		// console.log("prebook mapping", datapart.live_chunk_mapping);
		// console.log(datapart);
		let val = (datapart.forced_max_live_slots_count - datapart.forced_live_slots_count);
		return _.isNumber(datapart.forced_max_live_slots_count) ? val : _.clamp(_.sumBy(datapart.live_chunk_mapping, t => (_.floor(t / datapart.live_slot_size) - 1)), 0, Infinity);
	}
};