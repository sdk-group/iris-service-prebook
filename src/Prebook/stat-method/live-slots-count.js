module.exports = {
	name: 'live_slots_count',
	compute: function (datapart) {
		// console.log("prebook mapping", datapart.live_chunk_mapping);
		// console.log(datapart);
		return _.clamp(_.sumBy(datapart.live_chunk_mapping, t => (_.floor(t / datapart.live_slot_size) - 1)), 0, Infinity);
	}
};